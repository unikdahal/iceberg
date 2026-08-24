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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.connector.write.BatchWriteRecoveryState;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.RecoveryDataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestSparkWriteRecovery {
  @TempDir private Path tempDir;

  private Table table;

  @BeforeEach
  public void createTable() {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    table =
        new HadoopTables(new Configuration())
            .create(schema, PartitionSpec.unpartitioned(), tempDir.resolve("table").toString());
  }

  @Test
  public void taskCommitCodecIsPortableAndRejectsCorruption() throws Exception {
    SparkWriteRecoveryTaskCodec codec = new SparkWriteRecoveryTaskCodec(table);
    assertThat(codec.codecId()).isEqualTo("iceberg-data-files");
    assertThat(codec.version()).isPositive();

    ByteArrayOutputStream serializedCodec = new ByteArrayOutputStream();
    try (ObjectOutputStream output = new ObjectOutputStream(serializedCodec)) {
      output.writeObject(codec);
    }
    SparkWriteRecoveryTaskCodec deserializedCodec;
    try (ObjectInputStream input =
        new ObjectInputStream(new ByteArrayInputStream(serializedCodec.toByteArray()))) {
      deserializedCodec = (SparkWriteRecoveryTaskCodec) input.readObject();
    }

    SparkWrite.TaskCommit original = taskCommit("codec.parquet", 11L);
    byte[] encoded = deserializedCodec.encode(original);
    SparkWrite.TaskCommit decoded =
        (SparkWrite.TaskCommit) deserializedCodec.decode(deserializedCodec.version(), encoded);
    assertThat(decoded.files())
        .extracting(file -> file.location().toString())
        .containsExactly(original.files()[0].location().toString());
    assertThat(decoded.files()[0].recordCount()).isEqualTo(11L);

    byte[] corrupt = Arrays.copyOf(encoded, encoded.length);
    corrupt[corrupt.length - 1] ^= 1;
    assertThatThrownBy(() -> deserializedCodec.decode(deserializedCodec.version(), corrupt))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Corrupt");

    assertThatThrownBy(() -> deserializedCodec.decode(deserializedCodec.version() + 1, encoded))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Unsupported");

    byte[] truncated = Arrays.copyOf(encoded, encoded.length - 1);
    assertThatThrownBy(() -> deserializedCodec.decode(deserializedCodec.version(), truncated))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("does not match");
  }

  @Test
  public void recoversCatalogAtomicGlobalCommit() {
    SparkWrite.TaskCommit task = taskCommit("committed.parquet", 7L);
    org.apache.iceberg.AppendFiles append = table.newAppend().appendFile(task.files()[0]);
    SparkWriteRecovery.markCommit(append, "write-2");
    append.commit();

    BatchWriteRecoveryState recovered = SparkWriteRecovery.recover(table, "write-2", 1);
    assertThat(recovered.isCommitted()).isTrue();
    // Per-partition messages now live in the durable task store; the state carries only the
    // global-commit verdict and its authoritative row count.
    assertThat(recovered.totalNumRows()).isEqualTo(7L);
  }

  @Test
  public void concurrentSnapshotRetriesUseWriteIDAsAtomicIdempotencyKey() {
    SparkWrite.TaskCommit task = taskCommit("racing-commit.parquet", 5L);
    org.apache.iceberg.AppendFiles first = table.newAppend().appendFile(task.files()[0]);
    org.apache.iceberg.AppendFiles racing = table.newAppend().appendFile(task.files()[0]);
    SparkWriteRecovery.markCommit(first, "write-race");
    SparkWriteRecovery.markCommit(racing, "write-race");

    first.commit();
    long committedSnapshotID = table.currentSnapshot().snapshotId();
    racing.commit();
    table.refresh();

    assertThat(table.snapshots()).hasSize(1);
    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(committedSnapshotID);
    assertThat(table.currentSnapshot().summary())
        .containsEntry(SparkWriteRecovery.SNAPSHOT_WRITE_ID, "write-race");
  }

  @Test
  public void idempotencyLedgerSurvivesSnapshotExpiration() {
    SparkWrite.TaskCommit task = taskCommit("expired-commit.parquet", 5L);
    org.apache.iceberg.AppendFiles first = table.newAppend().appendFile(task.files()[0]);
    org.apache.iceberg.AppendFiles delayedRetry = table.newAppend().appendFile(task.files()[0]);
    SparkWriteRecovery.markCommit(first, "write-expired");
    SparkWriteRecovery.markCommit(delayedRetry, "write-expired");

    first.commit();
    long committedSnapshotID = table.currentSnapshot().snapshotId();
    table.newAppend().appendFile(taskCommit("new-current.parquet", 1L).files()[0]).commit();
    long currentSnapshotID = table.currentSnapshot().snapshotId();
    table.expireSnapshots().expireSnapshotId(committedSnapshotID).commit();
    table.refresh();
    assertThat(table.snapshot(committedSnapshotID)).isNull();

    BatchWriteRecoveryState recovered = SparkWriteRecovery.recover(table, "write-expired", 1);
    assertThat(recovered.isCommitted()).isTrue();
    assertThat(recovered.totalNumRows()).isEqualTo(5L);

    delayedRetry.commit();
    table.refresh();
    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(currentSnapshotID);
    assertThat(table.snapshots()).hasSize(1);
  }

  @Test
  public void corruptIdempotencyPropertyMakesRecoveryFailClosed() {
    table
        .updateProperties()
        .set(TableProperties.COMMIT_IDEMPOTENCY_ENTRY_PREFIX + "corrupt", "not-base64")
        .commit();

    // A corrupt entry can fail at either gate - undecodable bytes or decoded-but-invalid
    // content - and both must refuse to report a committed write.
    assertThatThrownBy(() -> SparkWriteRecovery.recover(table, "unknown-write", 1))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("idempotency ledger");
  }

  @Test
  public void compatibilityMetadataPinsOverwriteStateButAllowsConcurrentAppend() {
    byte[] appendBefore = compatibility("append", true, new byte[0]);
    byte[] overwriteBefore = compatibility("dynamic-overwrite", false, new byte[0]);
    table.newAppend().appendFile(taskCommit("concurrent.parquet", 1L).files()[0]).commit();
    table.refresh();

    assertThat(compatibility("append", true, new byte[0])).isEqualTo(appendBefore);
    assertThat(compatibility("dynamic-overwrite", false, new byte[0]))
        .isNotEqualTo(overwriteBefore);
    assertThat(compatibility("overwrite-by-filter", false, new byte[0]))
        .isNotEqualTo(compatibility("dynamic-overwrite", false, new byte[0]));
  }

  @Test
  public void overwriteCompatibilityPinsFilterIsolationAndValidationSnapshot() {
    byte[] first =
        SparkWriteRecoveryCompatibility.encodeOverwriteFilter(
            "{\"type\":\"eq\",\"term\":\"id\",\"value\":1}", "SERIALIZABLE", 10L);
    byte[] same =
        SparkWriteRecoveryCompatibility.encodeOverwriteFilter(
            "{\"type\":\"eq\",\"term\":\"id\",\"value\":1}", "SERIALIZABLE", 10L);
    byte[] changedFilter =
        SparkWriteRecoveryCompatibility.encodeOverwriteFilter(
            "{\"type\":\"eq\",\"term\":\"id\",\"value\":2}", "SERIALIZABLE", 10L);
    byte[] changedIsolation =
        SparkWriteRecoveryCompatibility.encodeOverwriteFilter(
            "{\"type\":\"eq\",\"term\":\"id\",\"value\":1}", "SNAPSHOT", 10L);
    byte[] changedValidation =
        SparkWriteRecoveryCompatibility.encodeOverwriteFilter(
            "{\"type\":\"eq\",\"term\":\"id\",\"value\":1}", "SERIALIZABLE", 11L);

    assertThat(first).isEqualTo(same);
    assertThat(first).isNotEqualTo(changedFilter);
    assertThat(first).isNotEqualTo(changedIsolation);
    assertThat(first).isNotEqualTo(changedValidation);
  }

  @Test
  public void compatibilityMetadataCanonicalizesAndPinsWriterProperties() {
    Map<String, String> first = new LinkedHashMap<>();
    first.put("z-property", "last");
    first.put("a-property", "first");
    Map<String, String> reordered = new LinkedHashMap<>();
    reordered.put("a-property", "first");
    reordered.put("z-property", "last");
    Map<String, String> changed = new LinkedHashMap<>(reordered);
    changed.put("z-property", "changed");

    assertThat(compatibility("append", true, new byte[0], first))
        .isEqualTo(compatibility("append", true, new byte[0], reordered));
    assertThat(compatibility("append", true, new byte[0], first))
        .isNotEqualTo(compatibility("append", true, new byte[0], changed));
  }

  private byte[] compatibility(String operation, boolean concurrent, byte[] operationMetadata) {
    return compatibility(operation, concurrent, operationMetadata, Collections.emptyMap());
  }

  private byte[] compatibility(
      String operation,
      boolean concurrent,
      byte[] operationMetadata,
      Map<String, String> writeProperties) {
    return SparkWriteRecoveryCompatibility.encode(
        table,
        table.schema(),
        table.spec(),
        table.sortOrder(),
        "PARQUET",
        128L * 1024 * 1024,
        false,
        writeProperties,
        Collections.emptyMap(),
        null,
        false,
        null,
        operation,
        concurrent,
        operationMetadata);
  }

  private byte[] compatibility(
      Schema schema,
      PartitionSpec spec,
      SortOrder sortOrder,
      String fileFormat,
      String branch,
      boolean wapEnabled,
      String wapID,
      boolean allowConcurrentSnapshots) {
    return SparkWriteRecoveryCompatibility.encode(
        table,
        schema,
        spec,
        sortOrder,
        fileFormat,
        128L * 1024 * 1024,
        false,
        Collections.emptyMap(),
        Collections.emptyMap(),
        branch,
        wapEnabled,
        wapID,
        "append",
        allowConcurrentSnapshots,
        new byte[0]);
  }

  @Test
  public void schemaDriftChangesTheDurableManifest() {
    Schema drifted =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "payload", Types.StringType.get()));

    assertThat(
            compatibility(
                drifted, table.spec(), table.sortOrder(), "PARQUET", null, false, null, true))
        .isNotEqualTo(
            compatibility(
                table.schema(),
                table.spec(),
                table.sortOrder(),
                "PARQUET",
                null,
                false,
                null,
                true));
  }

  @Test
  public void partitionSpecDriftChangesTheDurableManifest() {
    PartitionSpec drifted = PartitionSpec.builderFor(table.schema()).bucket("id", 8).build();

    assertThat(
            compatibility(
                table.schema(), drifted, table.sortOrder(), "PARQUET", null, false, null, true))
        .isNotEqualTo(
            compatibility(
                table.schema(),
                table.spec(),
                table.sortOrder(),
                "PARQUET",
                null,
                false,
                null,
                true));
  }

  @Test
  public void sortOrderDriftChangesTheDurableManifest() {
    SortOrder drifted = SortOrder.builderFor(table.schema()).asc("id").build();

    assertThat(
            compatibility(
                table.schema(), table.spec(), drifted, "PARQUET", null, false, null, true))
        .isNotEqualTo(
            compatibility(
                table.schema(),
                table.spec(),
                table.sortOrder(),
                "PARQUET",
                null,
                false,
                null,
                true));
  }

  @Test
  public void fileFormatDriftChangesTheDurableManifest() {
    assertThat(
            compatibility(
                table.schema(), table.spec(), table.sortOrder(), "AVRO", null, false, null, true))
        .isNotEqualTo(
            compatibility(
                table.schema(),
                table.spec(),
                table.sortOrder(),
                "PARQUET",
                null,
                false,
                null,
                true));
  }

  @Test
  public void branchDriftChangesTheDurableManifest() {
    assertThat(
            compatibility(
                table.schema(),
                table.spec(),
                table.sortOrder(),
                "PARQUET",
                "main",
                false,
                null,
                true))
        .isNotEqualTo(
            compatibility(
                table.schema(),
                table.spec(),
                table.sortOrder(),
                "PARQUET",
                null,
                false,
                null,
                true));
  }

  @Test
  public void wapStateDriftChangesTheDurableManifest() {
    assertThat(
            compatibility(
                table.schema(),
                table.spec(),
                table.sortOrder(),
                "PARQUET",
                null,
                true,
                "wap-id",
                true))
        .isNotEqualTo(
            compatibility(
                table.schema(),
                table.spec(),
                table.sortOrder(),
                "PARQUET",
                null,
                false,
                null,
                true));
  }

  @Test
  public void baseSnapshotDriftChangesTheDurableManifest() {
    table.newAppend().appendFile(taskCommit("base-a.parquet", 1L).files()[0]).commit();
    byte[] firstDriver =
        compatibility(
            table.schema(), table.spec(), table.sortOrder(), "PARQUET", null, false, null, false);

    table.newAppend().appendFile(taskCommit("base-b.parquet", 2L).files()[0]).commit();
    byte[] replacementDriver =
        compatibility(
            table.schema(), table.spec(), table.sortOrder(), "PARQUET", null, false, null, false);

    assertThat(replacementDriver).isNotEqualTo(firstDriver);
  }

  /**
   * Exercises the losing-writer cleanup contract without a live Spark execution: the durable
   * arbitration hands the loser's own commit message back and Spark calls discard on it, so the
   * files named by that message must be deleted through the table's FileIO while every other
   * attempt's output survives.
   */
  private RecoveryDataWriter recoverableWriter(FileIO io) throws Exception {
    Class<?> impl =
        Class.forName("org.apache.iceberg.spark.source.SparkWrite$RecoverableDataWriter");
    Constructor<?> constructor = impl.getDeclaredConstructor(DataWriter.class, FileIO.class);
    constructor.setAccessible(true);
    return (RecoveryDataWriter) constructor.newInstance(mock(DataWriter.class), io);
  }

  private DataFile dataFile(Path path, long records) {
    return DataFiles.builder(table.spec())
        .withPath(path.toString())
        .withFileSizeInBytes(8L)
        .withRecordCount(records)
        .build();
  }

  @Test
  public void losingAttemptFilesAreDeletedAndWinnerFilesRemain() throws Exception {
    Path loser = tempDir.resolve("loser.parquet");
    Path winner = tempDir.resolve("winner.parquet");
    Files.writeString(loser, "loser output");
    Files.writeString(winner, "winner output");

    RecoveryDataWriter writer = recoverableWriter(table.io());
    writer.discardCommittedOutput(new SparkWrite.TaskCommit(new DataFile[] {dataFile(loser, 3L)}));

    assertThat(loser).doesNotExist();
    assertThat(winner).exists();
  }

  @Test
  public void failedDeletionLeavesOutputInPlaceWithoutMaskingArbitration() throws Exception {
    Path loser = tempDir.resolve("stuck.parquet");
    Files.writeString(loser, "undeletable output");
    FileIO failingIo =
        new FileIO() {
          @Override
          public InputFile newInputFile(String path) {
            return table.io().newInputFile(path);
          }

          @Override
          public OutputFile newOutputFile(String path) {
            return table.io().newOutputFile(path);
          }

          @Override
          public void deleteFile(String path) {
            throw new RuntimeException("disk refused");
          }
        };

    RecoveryDataWriter writer = recoverableWriter(failingIo);
    writer.discardCommittedOutput(new SparkWrite.TaskCommit(new DataFile[] {dataFile(loser, 3L)}));

    assertThat(loser).exists();
  }

  @Test
  public void discardRejectsForeignCommitMessages() throws Exception {
    RecoveryDataWriter writer = recoverableWriter(table.io());
    assertThatThrownBy(() -> writer.discardCommittedOutput(mock(WriterCommitMessage.class)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Invalid Iceberg recovery commit message");
  }

  private SparkWrite.TaskCommit taskCommit(String name, long records) {
    DataFile file =
        DataFiles.builder(table.spec())
            .withPath(tempDir.resolve(name).toString())
            .withFileSizeInBytes(10L)
            .withRecordCount(records)
            .build();
    return new SparkWrite.TaskCommit(new DataFile[] {file});
  }
}
