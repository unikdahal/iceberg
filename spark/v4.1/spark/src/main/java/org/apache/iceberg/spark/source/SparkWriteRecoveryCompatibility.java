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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** Stable compatibility metadata for a recoverable Iceberg Spark write. */
final class SparkWriteRecoveryCompatibility {
  private static final int MAGIC = 0x4957434D; // IWCM
  private static final int VERSION = 1;

  private SparkWriteRecoveryCompatibility() {}

  static byte[] encode(
      Table table,
      Schema writeSchema,
      PartitionSpec outputSpec,
      SortOrder outputSortOrder,
      String fileFormat,
      long targetFileSize,
      boolean fanoutEnabled,
      Map<String, String> writeProperties,
      Map<String, String> snapshotProperties,
      String branch,
      boolean wapEnabled,
      String wapID,
      String operation,
      boolean allowConcurrentSnapshots,
      byte[] operationMetadata) {
    Preconditions.checkArgument(operation != null, "Invalid null recovery operation");
    Preconditions.checkArgument(operationMetadata != null, "Invalid null operation metadata");
    Preconditions.checkArgument(writeSchema != null, "Invalid null write schema");
    Preconditions.checkArgument(outputSpec != null, "Invalid null output partition spec");
    Preconditions.checkArgument(outputSortOrder != null, "Invalid null output sort order");
    Preconditions.checkArgument(fileFormat != null, "Invalid null file format");
    Preconditions.checkArgument(targetFileSize > 0, "Invalid target file size: %s", targetFileSize);
    UUID tableUUID = table.uuid();
    Preconditions.checkState(tableUUID != null, "Cannot recover a table without a stable UUID");
    Snapshot currentSnapshot = branch != null ? table.snapshot(branch) : table.currentSnapshot();
    long baseSnapshotID =
        allowConcurrentSnapshots || currentSnapshot == null ? -1L : currentSnapshot.snapshotId();

    try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream data = new DataOutputStream(bytes)) {
      data.writeInt(MAGIC);
      data.writeInt(VERSION);
      data.writeLong(tableUUID.getMostSignificantBits());
      data.writeLong(tableUUID.getLeastSignificantBits());
      // Encode both IDs and definitions. IDs protect table-local identity while definitions make
      // compatibility independent of metadata pruning and fail closed if an ID is ever reused.
      data.writeInt(writeSchema.schemaId());
      writeString(data, SchemaParser.toJson(writeSchema));
      data.writeInt(outputSpec.specId());
      writeString(data, PartitionSpecParser.toJson(outputSpec));
      data.writeInt(outputSortOrder.orderId());
      writeString(data, SortOrderParser.toJson(outputSortOrder));
      writeString(data, fileFormat);
      data.writeLong(targetFileSize);
      data.writeBoolean(fanoutEnabled);
      writeMap(data, writeProperties);
      writeMap(data, snapshotProperties);
      writeNullableString(data, branch);
      data.writeBoolean(wapEnabled);
      writeNullableString(data, wapID);
      writeString(data, operation);
      data.writeBoolean(allowConcurrentSnapshots);
      data.writeLong(baseSnapshotID);
      data.writeInt(operationMetadata.length);
      data.write(operationMetadata);
      data.flush();
      return bytes.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to encode Spark write compatibility metadata", e);
    }
  }

  static byte[] encodeOverwriteFilter(
      String expressionJson, String isolationLevel, Long validateFromSnapshotID) {
    Preconditions.checkArgument(expressionJson != null, "Invalid null overwrite expression");
    try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream data = new DataOutputStream(bytes)) {
      writeString(data, expressionJson);
      writeNullableString(data, isolationLevel);
      data.writeBoolean(validateFromSnapshotID != null);
      if (validateFromSnapshotID != null) {
        data.writeLong(validateFromSnapshotID);
      }
      data.flush();
      return bytes.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to encode overwrite compatibility metadata", e);
    }
  }

  private static void writeNullableString(DataOutputStream data, String value) throws IOException {
    data.writeBoolean(value != null);
    if (value != null) {
      writeString(data, value);
    }
  }

  private static void writeString(DataOutputStream data, String value) throws IOException {
    byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
    data.writeInt(encoded.length);
    data.write(encoded);
  }

  private static void writeMap(DataOutputStream data, Map<String, String> values)
      throws IOException {
    Preconditions.checkArgument(values != null, "Invalid null recovery property map");
    Map<String, String> sorted = new TreeMap<>(values);
    data.writeInt(sorted.size());
    for (Map.Entry<String, String> entry : sorted.entrySet()) {
      Preconditions.checkArgument(
          entry.getKey() != null && entry.getValue() != null,
          "Recovery properties must not contain null keys or values");
      writeString(data, entry.getKey());
      writeString(data, entry.getValue());
    }
  }
}
