/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.events.CreateSnapshotEvent;
import org.apache.iceberg.events.Listeners;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSnapshotIdempotency extends TestBase {
  private static final String KEY = "test.logical-write-id";

  @TestTemplate
  public void concurrentPrebuiltUpdatesCommitOneSnapshotAndOneEvent() {
    String value = "concurrent";
    AtomicInteger matchingEvents = new AtomicInteger();
    Listeners.register(
        (CreateSnapshotEvent event) -> {
          if (value.equals(event.summary().get(KEY))) {
            matchingEvents.incrementAndGet();
          }
        },
        CreateSnapshotEvent.class);

    AppendFiles first = table.newAppend().appendFile(FILE_A).idempotencyKey(KEY, value);
    AppendFiles racing = table.newAppend().appendFile(FILE_A).idempotencyKey(KEY, value);
    first.commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    racing.commit();

    assertThat(table.snapshots()).hasSize(1);
    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(snapshotId);
    assertThat(matchingEvents).hasValue(1);
  }

  @TestTemplate
  public void ledgerSurvivesSnapshotExpirationAndKeepsAddedRows() {
    String value = "expired-snapshot";
    AppendFiles first = table.newAppend().appendFile(FILE_A).idempotencyKey(KEY, value);
    AppendFiles delayed = table.newAppend().appendFile(FILE_A).idempotencyKey(KEY, value);
    first.commit();
    long firstSnapshotID = table.currentSnapshot().snapshotId();
    table.newAppend().appendFile(FILE_B).commit();
    long currentSnapshotID = table.currentSnapshot().snapshotId();
    table.expireSnapshots().expireSnapshotId(firstSnapshotID).commit();

    String ledger = table.properties().get(ledgerKey(KEY, value));
    assertThat(ledger).isNotNull();
    assertThat(decodeAddedRows(ledger)).isEqualTo(FILE_A.recordCount());
    delayed.commit();

    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(currentSnapshotID);
    assertThat(table.snapshots()).hasSize(1);
  }

  @TestTemplate
  public void commitRetryPublishesSnapshotAndLedgerAtomically() {
    table.ops().failCommits(1);
    table.newAppend().appendFile(FILE_A).idempotencyKey(KEY, "retry").commit();

    assertThat(table.currentSnapshot().summary()).containsEntry(KEY, "retry");
    assertThat(table.properties()).containsKey(ledgerKey(KEY, "retry"));
  }

  @TestTemplate
  public void corruptLedgerEntriesFailClosed() {
    assertCorruptLedgerRejected("not-base64");
    assertCorruptLedgerRejected(encodedLedger(2, KEY, "bad-version", 1L, 1L, 1L, false));
    assertCorruptLedgerRejected(encodedLedger(1, KEY, "trailing", 1L, 1L, 1L, true));

    String mismatched = encodedLedger(1, KEY, "different", 1L, 1L, 1L, false);
    String expectedKey = ledgerKey(KEY, "expected");
    table.updateProperties().set(expectedKey, mismatched).commit();
    assertThatThrownBy(
            () -> table.newAppend().appendFile(FILE_A).idempotencyKey(KEY, "next").commit())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("hash collision");
    table.updateProperties().remove(expectedKey).commit();
  }

  @TestTemplate
  public void maxLiveEntriesApplyBackpressureButExpiredEntriesDoNotCount() {
    table
        .updateProperties()
        .set(TableProperties.COMMIT_IDEMPOTENCY_MAX_ENTRIES, "1")
        .set(TableProperties.COMMIT_IDEMPOTENCY_RETENTION_MS, "1000")
        .commit();
    table.newAppend().appendFile(FILE_A).idempotencyKey(KEY, "live").commit();
    assertThatThrownBy(
            () -> table.newAppend().appendFile(FILE_B).idempotencyKey(KEY, "overflow").commit())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("maximum is 1");

    String liveKey = ledgerKey(KEY, "live");
    table.updateProperties().remove(liveKey).commit();
    String expiredKey = ledgerKey(KEY, "expired");
    table
        .updateProperties()
        .set(expiredKey, encodedLedger(1, KEY, "expired", 1L, 0L, 1L, false))
        .commit();
    table.newAppend().appendFile(FILE_B).idempotencyKey(KEY, "replacement").commit();
    assertThat(table.properties()).doesNotContainKey(expiredKey);
    assertThat(table.properties()).containsKey(ledgerKey(KEY, "replacement"));
  }

  @TestTemplate
  public void rejectsUnboundedIdempotencyFields() {
    String oversized = "x".repeat(4097);
    assertThatThrownBy(
            () -> table.newAppend().appendFile(FILE_A).idempotencyKey(KEY, oversized))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("too large");
  }

  private void assertCorruptLedgerRejected(String value) {
    String property = TableProperties.COMMIT_IDEMPOTENCY_ENTRY_PREFIX + "corrupt";
    table.updateProperties().set(property, value).commit();
    assertThatThrownBy(
            () -> table.newAppend().appendFile(FILE_A).idempotencyKey(KEY, "next").commit())
        .isInstanceOfAny(IllegalStateException.class, IllegalArgumentException.class);
    table.updateProperties().remove(property).commit();
  }

  private static String ledgerKey(String property, String value) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      DataOutputStream data = new DataOutputStream(bytes);
      writeString(data, property);
      writeString(data, value);
      data.close();
      byte[] hash = MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray());
      StringBuilder hex = new StringBuilder(hash.length * 2);
      for (byte next : hash) {
        hex.append(String.format(Locale.ROOT, "%02x", next & 0xff));
      }
      return TableProperties.COMMIT_IDEMPOTENCY_ENTRY_PREFIX + hex;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static String encodedLedger(
      int version,
      String property,
      String value,
      long snapshotId,
      long timestamp,
      long addedRows,
      boolean trailing) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      DataOutputStream data = new DataOutputStream(bytes);
      data.writeInt(version);
      writeString(data, property);
      writeString(data, value);
      data.writeLong(snapshotId);
      data.writeLong(timestamp);
      data.writeLong(addedRows);
      if (trailing) {
        data.writeByte(1);
      }
      data.close();
      return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes.toByteArray());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static long decodeAddedRows(String encoded) {
    try {
      java.io.DataInputStream data =
          new java.io.DataInputStream(
              new java.io.ByteArrayInputStream(Base64.getUrlDecoder().decode(encoded)));
      data.readInt();
      skipString(data);
      skipString(data);
      data.readLong();
      data.readLong();
      return data.readLong();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void writeString(DataOutputStream data, String value) throws Exception {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    data.writeInt(bytes.length);
    data.write(bytes);
  }

  private static void skipString(java.io.DataInputStream data) throws Exception {
    int length = data.readInt();
    data.skipBytes(length);
  }
}
