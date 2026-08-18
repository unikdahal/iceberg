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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetAddress;
import java.nio.file.Path;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Proves that {@link SupportsSnapshotId} closes the A-1 race described in its own javadoc: an
 * unpinned read's post-execution snapshot id must reflect the snapshot the read actually planned
 * against and executed over, not whatever happens to be current on the table by the time the
 * caller asks.
 *
 * <p>Without this fix, the only public path to a snapshot id
 * (`BatchScanExec.table()` cast to `SparkTable`, then `.table().currentSnapshot()`) drifts to a
 * newer snapshot if a commit lands between planning and the caller's post-execution check, even
 * though the query itself read the older data -- see `SparkBatchQueryScan`'s prior lack of any
 * public accessor for its already-correct, already-computed `snapshotId` field.
 */
public class TestSupportsSnapshotId {

  private static final Configuration CONF = new Configuration();
  private static final Schema SCHEMA =
      new Schema(optional(1, "id", Types.IntegerType.get()));

  @TempDir private Path temp;

  private static SparkSession spark = null;

  @BeforeAll
  public static void startSpark() {
    TestSupportsSnapshotId.spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.driver.host", InetAddress.getLoopbackAddress().getHostAddress())
            .config(TestBase.DISABLE_UI)
            .getOrCreate();
  }

  @AfterAll
  public static void stopSpark() {
    SparkSession currentSpark = TestSupportsSnapshotId.spark;
    TestSupportsSnapshotId.spark = null;
    currentSpark.stop();
  }

  private static SupportsSnapshotId scanOf(Dataset<Row> df) {
    scala.collection.Iterator<SparkPlan> leaves =
        df.queryExecution().executedPlan().collectLeaves().iterator();
    while (leaves.hasNext()) {
      SparkPlan leaf = leaves.next();
      if (leaf instanceof BatchScanExec) {
        Object scan = ((BatchScanExec) leaf).scan();
        assertThat(scan)
            .as("scan should implement SupportsSnapshotId")
            .isInstanceOf(SupportsSnapshotId.class);
        return (SupportsSnapshotId) scan;
      }
    }
    throw new IllegalStateException("no BatchScanExec found in executed plan");
  }

  @Test
  public void unpinnedReadReportsPlanTimeSnapshotNotLiveSnapshot() {
    String tableLocation = temp.resolve("iceberg-table").toFile().toString();

    HadoopTables tables = new HadoopTables(CONF);
    Table table = tables.create(SCHEMA, PartitionSpec.unpartitioned(), ImmutableMap.of(), tableLocation);

    // V1
    spark
        .createDataFrame(Lists.newArrayList(new SimpleIntRecord(1)), SimpleIntRecord.class)
        .select("id")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);
    long v1 = table.currentSnapshot().snapshotId();

    // plan the read against V1 -- do not execute yet. Note that merely building the executed
    // plan (scanOf) already triggers Iceberg's own planFiles() (via Scan.toBatch()), which is
    // what populates the ScanReport this fix reads from -- planning is resolved here, well
    // before collect() runs.
    Dataset<Row> df = spark.read().format("iceberg").load(tableLocation);
    SupportsSnapshotId scan = scanOf(df);

    // unpinned read: no snapshot was explicitly requested, so the pinned accessor stays null
    // throughout -- it is a different question from what was actually resolved and read
    assertThat(scan.snapshotId()).as("unpinned read: no explicit pin requested").isNull();

    assertThat(scan.resolvedSnapshotId())
        .as("resolvedSnapshotId() immediately after planning must already be V1")
        .isEqualTo(v1);

    // a second, unrelated commit lands before the already-planned read executes
    spark
        .createDataFrame(Lists.newArrayList(new SimpleIntRecord(2)), SimpleIntRecord.class)
        .select("id")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);
    table.refresh();
    long v2 = table.currentSnapshot().snapshotId();
    assertThat(v2).isNotEqualTo(v1);

    // the OLD, still-public path drifts to V2 here: table().currentSnapshot() is live
    assertThat(table.currentSnapshot().snapshotId())
        .as("sanity check: the table's live current snapshot has moved on to V2")
        .isEqualTo(v2);

    // execute the already-planned read: it reads V1's file, not V2's
    List<Row> rows = df.collectAsList();
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).getInt(0)).isEqualTo(1);

    // the fix: post-execution, the SAME scan object still reports the snapshot it actually
    // planned and read against (V1), not whatever is current on the table now (V2) -- unaffected
    // by the commit that landed in between
    assertThat(scan.resolvedSnapshotId()).as("post-execution resolvedSnapshotId() must still be "
        + "the planned snapshot, not the table's live current snapshot").isEqualTo(v1);
  }

  public static class SimpleIntRecord {
    private int id;

    public SimpleIntRecord() {}

    public SimpleIntRecord(int id) {
      this.id = id;
    }

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }
  }
}
