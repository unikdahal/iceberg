/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.source;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ContentFileParser;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;
import org.apache.spark.sql.connector.write.RecoveryCommitMessageCodec;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

/** Stable binary codec for an Iceberg Spark task commit message. */
final class SparkWriteRecoveryTaskCodec implements RecoveryCommitMessageCodec, Serializable {
  private static final long serialVersionUID = 1L;
  private static final String CODEC_ID = "iceberg-data-files";
  private static final int MAGIC = 0x49575443; // IWTC
  private static final int VERSION = 1;
  private static final int CHECKSUM_LENGTH = 32;
  private static final int MAX_PAYLOAD_LENGTH = 16 * 1024 * 1024;

  private final Table table;

  SparkWriteRecoveryTaskCodec(Table table) {
    this.table = SerializableTableWithSize.copyOf(table);
  }

  @Override
  public String codecId() {
    return CODEC_ID;
  }

  @Override
  public int version() {
    return VERSION;
  }

  @Override
  public byte[] encode(WriterCommitMessage commitMessage) {
    Preconditions.checkState(
        commitMessage instanceof SparkWrite.TaskCommit,
        "Invalid Iceberg Spark task commit message: %s",
        commitMessage != null ? commitMessage.getClass().getName() : "null");
    byte[] content = encodeContent(table, (SparkWrite.TaskCommit) commitMessage);
    Preconditions.checkState(
        content.length <= MAX_PAYLOAD_LENGTH,
        "Spark write recovery task payload is too large: %s bytes",
        content.length);

    try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream data = new DataOutputStream(bytes)) {
      data.writeInt(MAGIC);
      data.writeInt(VERSION);
      data.writeInt(content.length);
      data.write(content);
      data.write(sha256(content));
      data.flush();
      return bytes.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to encode Spark write task recovery state", e);
    }
  }

  WriterCommitMessage decode(byte[] encoded) {
    Preconditions.checkState(encoded != null, "Missing Spark write recovery task payload");
    Preconditions.checkState(
        encoded.length >= 3 * Integer.BYTES + CHECKSUM_LENGTH,
        "Truncated Spark write recovery task payload");
    try (DataInputStream data = new DataInputStream(new ByteArrayInputStream(encoded))) {
      Preconditions.checkState(data.readInt() == MAGIC, "Invalid Spark write recovery task magic");
      int version = data.readInt();
      Preconditions.checkState(
          version == VERSION, "Unsupported Spark write recovery task version: %s", version);
      int length = data.readInt();
      Preconditions.checkState(
          length >= 0 && length <= MAX_PAYLOAD_LENGTH,
          "Invalid Spark write recovery task payload length: %s",
          length);
      Preconditions.checkState(
          length == data.available() - CHECKSUM_LENGTH,
          "Spark write recovery task payload length does not match its envelope");
      byte[] content = new byte[length];
      data.readFully(content);
      byte[] checksum = new byte[CHECKSUM_LENGTH];
      data.readFully(checksum);
      Preconditions.checkState(
          MessageDigest.isEqual(checksum, sha256(content)),
          "Corrupt Spark write recovery task payload");
      Preconditions.checkState(data.read() == -1, "Trailing Spark write recovery task bytes");
      return decodeContent(table, content);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to decode Spark write task recovery state", e);
    }
  }

  @Override
  public WriterCommitMessage decode(int codecVersion, byte[] encoded) {
    Preconditions.checkState(
        codecVersion == VERSION,
        "Unsupported Iceberg Spark recovery codec version: %s",
        codecVersion);
    return decode(encoded);
  }

  private static byte[] encodeContent(Table table, SparkWrite.TaskCommit message) {
    try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream data = new DataOutputStream(bytes)) {
      DataFile[] files = message.files();
      data.writeInt(files.length);
      for (DataFile file : files) {
        byte[] json =
            ContentFileParser.toJson(file, table.specs().get(file.specId()))
                .getBytes(StandardCharsets.UTF_8);
        data.writeInt(json.length);
        data.write(json);
      }
      data.flush();
      return bytes.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to encode Spark write task recovery content", e);
    }
  }

  private static SparkWrite.TaskCommit decodeContent(Table table, byte[] content) {
    try (DataInputStream data = new DataInputStream(new ByteArrayInputStream(content))) {
      int numFiles = data.readInt();
      Preconditions.checkState(
          numFiles >= 0 && numFiles <= content.length / Integer.BYTES,
          "Invalid Spark write recovery file count: %s",
          numFiles);
      DataFile[] files = new DataFile[numFiles];
      for (int index = 0; index < numFiles; index += 1) {
        int length = data.readInt();
        Preconditions.checkState(
            length >= 0 && length <= data.available(),
            "Invalid Spark write recovery data file length: %s",
            length);
        byte[] json = new byte[length];
        data.readFully(json);
        ContentFile<?> file =
            JsonUtil.parse(
                new String(json, StandardCharsets.UTF_8),
                node -> ContentFileParser.fromJson(node, table.specs()));
        Preconditions.checkState(
            file instanceof DataFile,
            "Spark write recovery contains a non-data file at index %s",
            index);
        files[index] = (DataFile) file;
      }
      Preconditions.checkState(data.read() == -1, "Trailing Spark write recovery task content");
      return new SparkWrite.TaskCommit(files);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to decode Spark write task recovery content", e);
    }
  }

  private static byte[] sha256(byte[] value) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(value);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is unavailable", e);
    }
  }
}
