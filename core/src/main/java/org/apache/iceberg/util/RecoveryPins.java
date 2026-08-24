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
package org.apache.iceberg.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Recovery pins keep a selected snapshot readable for as long as an interrupted execution may be
 * resumed.
 *
 * <p>An engine that wants to resume a query after its driver dies must read exactly the snapshot
 * the first driver read. Selecting a snapshot ID is not enough on its own: ordinary snapshot
 * expiration can remove that snapshot while the driver is down, which turns a recoverable failure
 * into an unrecoverable one. A pin is a deterministic tag, derived from the execution and source
 * identity, that holds the snapshot for a bounded time.
 *
 * <p>Every operation here is idempotent and fails closed. Re-pinning the same snapshot succeeds;
 * pinning a different snapshot under the same identity fails rather than moving the pin, because a
 * moved pin would silently change what a resumed execution reads.
 *
 * <p>The pin is a normal Iceberg tag, so it participates in expiration and in {@code
 * expire_snapshots} exactly like any other reference, and a maximum reference age bounds how long
 * an abandoned execution can hold a snapshot.
 */
public class RecoveryPins {

  private static final String PIN_PREFIX = "recovery-";
  private static final int MAX_IDENTITY_BYTES = 4096;

  private RecoveryPins() {}

  /**
   * Returns the deterministic tag name for an execution and source identity.
   *
   * <p>The digest covers a length-delimited encoding of both identities, so no pair of identities
   * can produce the name of another pair, and the same pair always produces the same name in every
   * driver incarnation.
   */
  public static String pinName(String recoveryId, String sourceId) {
    validateIdentity("recovery ID", recoveryId);
    validateIdentity("source ID", sourceId);

    String material = recoveryId.length() + ":" + recoveryId + sourceId.length() + ":" + sourceId;
    byte[] digest = sha256(material.getBytes(StandardCharsets.UTF_8));
    StringBuilder name = new StringBuilder(PIN_PREFIX.length() + digest.length * 2);
    name.append(PIN_PREFIX);
    for (byte value : digest) {
      name.append(String.format(Locale.ROOT, "%02x", value & 0xff));
    }

    return name.toString();
  }

  /**
   * Pins {@code snapshotId} for this execution and source, and returns the pinned snapshot ID.
   *
   * <p>Idempotent: pinning a snapshot that is already pinned under this identity succeeds and only
   * extends the maximum reference age when the requested age is longer. Pinning a different
   * snapshot under an existing pin throws, so a replacement driver can never move a pin forward to
   * newer data.
   *
   * @param table the table holding the source snapshot
   * @param recoveryId stable identity of one immutable logical execution
   * @param sourceId stable identity of the source, excluding the selected version
   * @param snapshotId the snapshot the execution selected
   * @param maxRefAgeMs how long the pin may live; must exceed the recovery window
   */
  public static long pin(
      Table table, String recoveryId, String sourceId, long snapshotId, long maxRefAgeMs) {
    Preconditions.checkArgument(table != null, "Invalid table: null");
    Preconditions.checkArgument(maxRefAgeMs > 0, "Invalid maximum reference age: %s", maxRefAgeMs);
    Preconditions.checkArgument(
        table.snapshot(snapshotId) != null,
        "Cannot pin snapshot %s: not present in table %s",
        snapshotId,
        table.name());

    String name = pinName(recoveryId, sourceId);
    SnapshotRef existing = table.refs().get(name);
    if (existing != null) {
      validateExisting(name, existing, snapshotId, table.name());
      Long existingAge = existing.maxRefAgeMs();
      if (existingAge == null || existingAge < maxRefAgeMs) {
        table.manageSnapshots().setMaxRefAgeMs(name, maxRefAgeMs).commit();
      }

      return existing.snapshotId();
    }

    try {
      table
          .manageSnapshots()
          .createTag(name, snapshotId)
          .setMaxRefAgeMs(name, maxRefAgeMs)
          .commit();
    } catch (IllegalArgumentException | ValidationException e) {
      // Another driver may have created the same pin between the read and the commit. That is the
      // expected outcome of two drivers resuming the same execution, not a failure, as long as the
      // pin they created points at the same snapshot.
      table.refresh();
      SnapshotRef concurrent = table.refs().get(name);
      if (concurrent == null) {
        throw e;
      }

      validateExisting(name, concurrent, snapshotId, table.name());
      return concurrent.snapshotId();
    }

    return snapshotId;
  }

  /**
   * Verifies that the pin for this execution and source still points at {@code expectedSnapshotId}.
   *
   * <p>A missing pin, a pin that is not a tag, or a pin that points elsewhere all throw. A
   * replacement driver must fail rather than read different data than the execution it resumes.
   */
  public static long verify(
      Table table, String recoveryId, String sourceId, long expectedSnapshotId) {
    Preconditions.checkArgument(table != null, "Invalid table: null");

    String name = pinName(recoveryId, sourceId);
    SnapshotRef pin = table.refs().get(name);
    ValidationException.check(
        pin != null,
        "Recovery pin %s is missing from table %s: the selected snapshot is no longer protected",
        name,
        table.name());
    validateExisting(name, pin, expectedSnapshotId, table.name());
    ValidationException.check(
        table.snapshot(expectedSnapshotId) != null,
        "Snapshot %s referenced by recovery pin %s is missing from table %s",
        expectedSnapshotId,
        name,
        table.name());

    return pin.snapshotId();
  }

  /**
   * Releases the pin for this execution and source, returning whether a pin was removed.
   *
   * <p>Best effort by design: an execution that finished has no further use for its pin, and a pin
   * that survives a failed release is bounded by its maximum reference age.
   */
  public static boolean release(Table table, String recoveryId, String sourceId) {
    Preconditions.checkArgument(table != null, "Invalid table: null");

    String name = pinName(recoveryId, sourceId);
    SnapshotRef pin = table.refs().get(name);
    if (pin == null) {
      return false;
    }

    ValidationException.check(
        pin.isTag(), "Recovery pin %s in table %s is a branch, not a tag", name, table.name());
    table.manageSnapshots().removeTag(name).commit();
    return true;
  }

  private static void validateExisting(
      String name, SnapshotRef pin, long expectedSnapshotId, String tableName) {
    ValidationException.check(
        pin.isTag(), "Recovery pin %s in table %s is a branch, not a tag", name, tableName);
    ValidationException.check(
        pin.snapshotId() == expectedSnapshotId,
        "Recovery pin %s in table %s points at snapshot %s, expected %s",
        name,
        tableName,
        pin.snapshotId(),
        expectedSnapshotId);
  }

  private static void validateIdentity(String label, String value) {
    Preconditions.checkArgument(
        value != null && !value.isEmpty(), "Invalid %s: null or empty", label);
    Preconditions.checkArgument(
        value.getBytes(StandardCharsets.UTF_8).length <= MAX_IDENTITY_BYTES,
        "Invalid %s: longer than %s UTF-8 bytes",
        label,
        MAX_IDENTITY_BYTES);
  }

  private static byte[] sha256(byte[] material) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(material);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("JVM does not provide SHA-256", e);
    }
  }
}
