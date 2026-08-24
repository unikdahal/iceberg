/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
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
package org.apache.iceberg.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.TimeUnit;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.TestBase;
import org.apache.iceberg.exceptions.ValidationException;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestRecoveryPins extends TestBase {

  private static final String RECOVERY_ID = "workflow-run-17";
  private static final String SOURCE_ID = "catalog.db.table";
  private static final long MAX_REF_AGE_MS = TimeUnit.HOURS.toMillis(96);

  @TestTemplate
  public void pinNameIsDeterministicAndSeparatesIdentities() {
    String name = RecoveryPins.pinName(RECOVERY_ID, SOURCE_ID);

    assertThat(RecoveryPins.pinName(RECOVERY_ID, SOURCE_ID)).isEqualTo(name);
    assertThat(RecoveryPins.pinName(RECOVERY_ID, "catalog.db.other")).isNotEqualTo(name);
    assertThat(RecoveryPins.pinName("workflow-run-18", SOURCE_ID)).isNotEqualTo(name);
    // A concatenation without length delimiters would collide on these two.
    assertThat(RecoveryPins.pinName("a", "bc")).isNotEqualTo(RecoveryPins.pinName("ab", "c"));
  }

  @TestTemplate
  public void pinIsIdempotentForTheSameSnapshot() {
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();

    assertThat(RecoveryPins.pin(table, RECOVERY_ID, SOURCE_ID, snapshotId, MAX_REF_AGE_MS))
        .isEqualTo(snapshotId);
    assertThat(RecoveryPins.pin(table, RECOVERY_ID, SOURCE_ID, snapshotId, MAX_REF_AGE_MS))
        .isEqualTo(snapshotId);

    SnapshotRef pin = table.refs().get(RecoveryPins.pinName(RECOVERY_ID, SOURCE_ID));
    assertThat(pin.isTag()).isTrue();
    assertThat(pin.snapshotId()).isEqualTo(snapshotId);
    assertThat(pin.maxRefAgeMs()).isEqualTo(MAX_REF_AGE_MS);
  }

  @TestTemplate
  public void pinExtendsButNeverShortensTheMaximumReferenceAge() {
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    String name = RecoveryPins.pinName(RECOVERY_ID, SOURCE_ID);

    RecoveryPins.pin(table, RECOVERY_ID, SOURCE_ID, snapshotId, MAX_REF_AGE_MS);
    RecoveryPins.pin(table, RECOVERY_ID, SOURCE_ID, snapshotId, TimeUnit.HOURS.toMillis(1));
    assertThat(table.refs().get(name).maxRefAgeMs()).isEqualTo(MAX_REF_AGE_MS);

    long longer = TimeUnit.DAYS.toMillis(30);
    RecoveryPins.pin(table, RECOVERY_ID, SOURCE_ID, snapshotId, longer);
    assertThat(table.refs().get(name).maxRefAgeMs()).isEqualTo(longer);
  }

  @TestTemplate
  public void pinRefusesToMoveToAnotherSnapshot() {
    table.newFastAppend().appendFile(FILE_A).commit();
    long first = table.currentSnapshot().snapshotId();
    RecoveryPins.pin(table, RECOVERY_ID, SOURCE_ID, first, MAX_REF_AGE_MS);

    table.newFastAppend().appendFile(FILE_B).commit();
    long second = table.currentSnapshot().snapshotId();

    assertThatThrownBy(
            () -> RecoveryPins.pin(table, RECOVERY_ID, SOURCE_ID, second, MAX_REF_AGE_MS))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("points at snapshot");
    assertThat(table.refs().get(RecoveryPins.pinName(RECOVERY_ID, SOURCE_ID)).snapshotId())
        .isEqualTo(first);
  }

  @TestTemplate
  public void pinRejectsASnapshotTheTableDoesNotHave() {
    table.newFastAppend().appendFile(FILE_A).commit();

    assertThatThrownBy(() -> RecoveryPins.pin(table, RECOVERY_ID, SOURCE_ID, -1L, MAX_REF_AGE_MS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not present in table");
  }

  @TestTemplate
  public void expirationCannotRemoveAPinnedSnapshot() {
    table.newFastAppend().appendFile(FILE_A).commit();
    long pinned = table.currentSnapshot().snapshotId();
    RecoveryPins.pin(table, RECOVERY_ID, SOURCE_ID, pinned, MAX_REF_AGE_MS);

    table.newFastAppend().appendFile(FILE_B).commit();
    long current = table.currentSnapshot().snapshotId();

    // Expire everything the main branch no longer needs. Without the pin this removes the snapshot
    // an interrupted execution was reading, which is exactly the availability gap pins close.
    table.expireSnapshots().expireOlderThan(System.currentTimeMillis()).commit();

    assertThat(table.snapshot(pinned)).isNotNull();
    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(current);
    assertThat(RecoveryPins.verify(table, RECOVERY_ID, SOURCE_ID, pinned)).isEqualTo(pinned);
  }

  @TestTemplate
  public void verifyFailsClosedWhenThePinIsMissingOrMoved() {
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();

    assertThatThrownBy(() -> RecoveryPins.verify(table, RECOVERY_ID, SOURCE_ID, snapshotId))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("is missing from table");

    RecoveryPins.pin(table, RECOVERY_ID, SOURCE_ID, snapshotId, MAX_REF_AGE_MS);
    table.newFastAppend().appendFile(FILE_B).commit();
    long other = table.currentSnapshot().snapshotId();

    assertThatThrownBy(() -> RecoveryPins.verify(table, RECOVERY_ID, SOURCE_ID, other))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("points at snapshot");
  }

  @TestTemplate
  public void releaseRemovesThePinAndIsIdempotent() {
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    RecoveryPins.pin(table, RECOVERY_ID, SOURCE_ID, snapshotId, MAX_REF_AGE_MS);

    assertThat(RecoveryPins.release(table, RECOVERY_ID, SOURCE_ID)).isTrue();
    assertThat(table.refs()).doesNotContainKey(RecoveryPins.pinName(RECOVERY_ID, SOURCE_ID));
    assertThat(RecoveryPins.release(table, RECOVERY_ID, SOURCE_ID)).isFalse();
  }

  @TestTemplate
  public void identitiesAreValidated() {
    assertThatThrownBy(() -> RecoveryPins.pinName("", SOURCE_ID))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("recovery ID");
    assertThatThrownBy(() -> RecoveryPins.pinName(RECOVERY_ID, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("source ID");
  }
}
