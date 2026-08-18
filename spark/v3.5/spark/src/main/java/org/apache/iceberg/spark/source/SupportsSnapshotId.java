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

/**
 * A {@link org.apache.spark.sql.connector.read.Scan} that can report the snapshot id it was
 * explicitly pinned to, and separately, the snapshot id it actually planned and resolved against.
 *
 * <p>These are deliberately two different questions:
 *
 * <ul>
 *   <li>{@link #snapshotId()} -- the snapshot the read explicitly requested (e.g. {@code VERSION
 *       AS OF <id>} / the {@code snapshot-id} read option). {@code null} for an unpinned read: no
 *       id was requested, so none is reported here.
 *   <li>{@link #resolvedSnapshotId()} -- the snapshot this scan actually planned and read against,
 *       whichever way it got there. For a pinned read this is the same value as {@link
 *       #snapshotId()}. For an unpinned read, it is resolved once when the scan is planned, and
 *       fixed from then on -- unlike asking the {@link SparkTable} the scan was built from for
 *       {@code table().currentSnapshot()} after the fact, which keeps tracking the table's live
 *       state and can have moved on by the time a caller checks it. A caller that needs to know
 *       what a completed scan actually read (lineage/provenance tooling, caching or resumption
 *       layers, audit logging) should use {@link #resolvedSnapshotId()}, not re-query the table's
 *       live state. Returns {@code null} before this scan has been planned, or for a scan type
 *       with no single-snapshot notion (e.g. an incremental scan between two snapshots).
 * </ul>
 */
public interface SupportsSnapshotId {
  Long snapshotId();

  Long resolvedSnapshotId();
}
