/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iceberg.spark.source;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.spark.sql.connector.write.BatchWriteRecoveryState;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

/** Catalog-atomic global commit state for a resumable Spark batch write. */
final class SparkWriteRecovery {
  static final String SNAPSHOT_WRITE_ID = "spark.sql.batch.write-id";
  private static final int LEDGER_VERSION = 1;
  private static final int MAX_LEDGER_ENTRY_BYTES = 1024 * 1024;
  private static final int MAX_LEDGER_ENCODED_CHARS = 2 * 1024 * 1024;
  private static final int MAX_LEDGER_FIELD_BYTES = 4096;

  private SparkWriteRecovery() {}

  static BatchWriteRecoveryState recover(Table table, String writeID, int numPartitions) {
    Preconditions.checkArgument(numPartitions >= 0, "Invalid partition count: %s", numPartitions);
    table.refresh();
    CommitState committed = committedState(table, writeID);
    WriterCommitMessage[] messages = new WriterCommitMessage[numPartitions];
    long[] numRows = new long[numPartitions];
    Arrays.fill(numRows, -1L);
    return new BatchWriteRecoveryState() {
      @Override
      public boolean isCommitted() {
        return committed != null;
      }

      @Override
      public WriterCommitMessage[] commitMessages() {
        return messages;
      }

      @Override
      public long[] numRows() {
        return numRows;
      }

      @Override
      public long totalNumRows() {
        return committed != null ? committed.addedRows : -1L;
      }
    };
  }

  static void markCommit(SnapshotUpdate<?> operation, String writeID) {
    operation.idempotencyKey(SNAPSHOT_WRITE_ID, writeID);
  }

  static boolean isCommitted(Table table, String writeID) {
    table.refresh();
    return committedState(table, writeID) != null;
  }

  private static CommitState committedState(Table table, String writeID) {
    Snapshot committedSnapshot = null;
    for (Snapshot snapshot : table.snapshots()) {
      if (writeID.equals(snapshot.summary().get(SNAPSHOT_WRITE_ID))) {
        Preconditions.checkState(
            committedSnapshot == null,
            "Multiple Iceberg snapshots use Spark recovery write ID %s",
            writeID);
        committedSnapshot = snapshot;
      }
    }

    CommitState ledgerState = null;
    for (Map.Entry<String, String> property : table.properties().entrySet()) {
      if (property.getKey().startsWith(TableProperties.COMMIT_IDEMPOTENCY_ENTRY_PREFIX)) {
        CommitState candidate = decodeLedgerEntry(property.getKey(), property.getValue());
        if (SNAPSHOT_WRITE_ID.equals(candidate.property) && writeID.equals(candidate.value)) {
          Preconditions.checkState(
              ledgerState == null,
              "Multiple Iceberg idempotency ledger entries use Spark recovery write ID %s",
              writeID);
          ledgerState = candidate;
        }
      }
    }

    if (committedSnapshot != null && ledgerState != null) {
      Preconditions.checkState(
          committedSnapshot.snapshotId() == ledgerState.snapshotID,
          "Iceberg snapshot and idempotency ledger disagree for Spark recovery write ID %s",
          writeID);
      long snapshotRows = committedRows(committedSnapshot);
      Preconditions.checkState(
          snapshotRows < 0 || ledgerState.addedRows < 0 || snapshotRows == ledgerState.addedRows,
          "Iceberg snapshot and idempotency ledger row counts disagree for Spark recovery write ID %s",
          writeID);
    }

    return ledgerState != null
        ? ledgerState
        : committedSnapshot != null
            ? new CommitState(
                SNAPSHOT_WRITE_ID,
                writeID,
                committedSnapshot.snapshotId(),
                committedRows(committedSnapshot))
            : null;
  }

  private static CommitState decodeLedgerEntry(String key, String encoded) {
    Preconditions.checkState(encoded != null, "Missing Iceberg idempotency ledger value");
    Preconditions.checkState(
        encoded.length() <= MAX_LEDGER_ENCODED_CHARS,
        "Iceberg idempotency ledger encoding is too large: %s characters",
        encoded.length());
    final byte[] bytes;
    try {
      bytes = Base64.getUrlDecoder().decode(encoded);
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException("Invalid Iceberg idempotency ledger encoding", e);
    }
    Preconditions.checkState(
        bytes.length <= MAX_LEDGER_ENTRY_BYTES,
        "Iceberg idempotency ledger entry is too large: %s bytes",
        bytes.length);

    try (DataInputStream data = new DataInputStream(new ByteArrayInputStream(bytes))) {
      int version = data.readInt();
      Preconditions.checkState(
          version == LEDGER_VERSION, "Unsupported Iceberg idempotency ledger version: %s", version);
      String property = readString(data, MAX_LEDGER_FIELD_BYTES);
      String value = readString(data, MAX_LEDGER_FIELD_BYTES);
      long snapshotID = data.readLong();
      long committedAtMillis = data.readLong();
      long addedRows = data.readLong();
      Preconditions.checkState(snapshotID >= 0, "Invalid Iceberg idempotency snapshot ID");
      Preconditions.checkState(committedAtMillis >= 0, "Invalid Iceberg idempotency timestamp");
      Preconditions.checkState(addedRows >= -1, "Invalid Iceberg idempotency row count");
      Preconditions.checkState(data.read() == -1, "Trailing Iceberg idempotency ledger bytes");
      Preconditions.checkState(
          key.equals(ledgerKey(property, value)),
          "Iceberg idempotency ledger key does not match its payload");
      return new CommitState(property, value, snapshotID, addedRows);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to decode Iceberg idempotency ledger entry", e);
    }
  }

  private static String readString(DataInputStream data, int maximumLength) throws IOException {
    int length = data.readInt();
    Preconditions.checkState(
        length >= 0 && length <= maximumLength && length <= data.available(),
        "Invalid Iceberg idempotency ledger field length: %s",
        length);
    byte[] value = new byte[length];
    data.readFully(value);
    try {
      return StandardCharsets.UTF_8
          .newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
          .decode(ByteBuffer.wrap(value))
          .toString();
    } catch (CharacterCodingException e) {
      throw new IllegalStateException("Invalid UTF-8 in Iceberg idempotency ledger entry", e);
    }
  }

  private static String ledgerKey(String property, String value) {
    try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream data = new DataOutputStream(bytes)) {
      writeString(data, property);
      writeString(data, value);
      data.flush();
      return TableProperties.COMMIT_IDEMPOTENCY_ENTRY_PREFIX + hex(sha256(bytes.toByteArray()));
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to build Iceberg idempotency ledger key", e);
    }
  }

  private static void writeString(DataOutputStream data, String value) throws IOException {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    data.writeInt(bytes.length);
    data.write(bytes);
  }

  private static byte[] sha256(byte[] value) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(value);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is unavailable", e);
    }
  }

  private static String hex(byte[] value) {
    StringBuilder builder = new StringBuilder(value.length * 2);
    for (byte next : value) {
      builder.append(String.format(Locale.ROOT, "%02x", next & 0xff));
    }
    return builder.toString();
  }

  private static long committedRows(Snapshot snapshot) {
    if (snapshot == null) {
      return -1L;
    }

    String rows = snapshot.summary().get("added-records");
    if (rows == null) {
      return -1L;
    }

    try {
      return Long.parseLong(rows);
    } catch (NumberFormatException e) {
      return -1L;
    }
  }

  private static class CommitState {
    private final String property;
    private final String value;
    private final long snapshotID;
    private final long addedRows;

    private CommitState(String property, String value, long snapshotID, long addedRows) {
      this.property = property;
      this.value = value;
      this.snapshotID = snapshotID;
      this.addedRows = addedRows;
    }
  }
}
