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

import static org.apache.iceberg.TableProperties.FORMAT_VERSION;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.BaseView;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewOperations;
import org.apache.spark.sql.types.StructType;

/**
 * Exposes an Iceberg view as Spark's connector {@link org.apache.spark.sql.connector.catalog.View}.
 *
 * <p>Spark 5 turned that interface into a builder-built class, so this wrapper both extends it -
 * supplying the builder with everything Iceberg can answer - and overrides every accessor with
 * values read straight from the Iceberg view, so behaviour never depends on what the builder kept.
 */
public class SparkView extends org.apache.spark.sql.connector.catalog.View {

  public static final String QUERY_COLUMN_NAMES = "spark.query-column-names";
  public static final Set<String> RESERVED_PROPERTIES =
      ImmutableSet.of("provider", "location", FORMAT_VERSION, QUERY_COLUMN_NAMES);

  private final View icebergView;
  private final String catalogName;
  private StructType lazySchema = null;

  public SparkView(String catalogName, View icebergView) {
    super(builder(catalogName, icebergView));
    this.catalogName = catalogName;
    this.icebergView = icebergView;
  }

  private static org.apache.spark.sql.connector.catalog.View.Builder builder(
      String catalogName, View icebergView) {
    org.apache.spark.sql.connector.catalog.View.Builder builder =
        new org.apache.spark.sql.connector.catalog.View.Builder()
            .withCurrentCatalog(
                icebergView.currentVersion().defaultCatalog() != null
                    ? icebergView.currentVersion().defaultCatalog()
                    : catalogName)
            .withCurrentNamespace(icebergView.currentVersion().defaultNamespace().levels())
            .withProperties(icebergView.properties())
            .withSchema(SparkSchemaUtil.convert(icebergView.schema()))
            .withSqlConfigs(Collections.emptyMap());
    if (icebergView.properties().containsKey(QUERY_COLUMN_NAMES)) {
      builder.withQueryColumnNames(
          icebergView.properties().get(QUERY_COLUMN_NAMES).split(","));
    }
    SQLViewRepresentation sqlRepr = icebergView.sqlFor("spark");
    if (sqlRepr != null) {
      builder.withQueryText(sqlRepr.sql());
    }
    return builder;
  }

  public View view() {
    return icebergView;
  }

  public String name() {
    return icebergView.name();
  }

  @Override
  public String queryText() {
    SQLViewRepresentation sqlRepr = icebergView.sqlFor("spark");
    Preconditions.checkState(sqlRepr != null, "Cannot load SQL for view %s", name());
    return sqlRepr.sql();
  }

  @Override
  public String currentCatalog() {
    return icebergView.currentVersion().defaultCatalog() != null
        ? icebergView.currentVersion().defaultCatalog()
        : catalogName;
  }

  @Override
  public String[] currentNamespace() {
    return icebergView.currentVersion().defaultNamespace().levels();
  }

  @Override
  public StructType schema() {
    if (null == lazySchema) {
      this.lazySchema = SparkSchemaUtil.convert(icebergView.schema());
    }

    return lazySchema;
  }

  @Override
  public String[] queryColumnNames() {
    return icebergView.properties().containsKey(QUERY_COLUMN_NAMES)
        ? icebergView.properties().get(QUERY_COLUMN_NAMES).split(",")
        : new String[0];
  }

  public String[] columnAliases() {
    return icebergView.schema().columns().stream()
        .map(Types.NestedField::name)
        .toArray(String[]::new);
  }

  public String[] columnComments() {
    return icebergView.schema().columns().stream()
        .map(Types.NestedField::doc)
        .toArray(String[]::new);
  }

  @Override
  public Map<String, String> properties() {
    ImmutableMap.Builder<String, String> propsBuilder = ImmutableMap.builder();

    propsBuilder.put("provider", "iceberg");
    propsBuilder.put("location", icebergView.location());

    if (icebergView instanceof BaseView) {
      ViewOperations ops = ((BaseView) icebergView).operations();
      propsBuilder.put(FORMAT_VERSION, String.valueOf(ops.current().formatVersion()));
    }

    icebergView.properties().entrySet().stream()
        .filter(entry -> !RESERVED_PROPERTIES.contains(entry.getKey()))
        .forEach(propsBuilder::put);

    return propsBuilder.build();
  }

  @Override
  public String toString() {
    return icebergView.toString();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (other == null || getClass() != other.getClass()) {
      return false;
    }

    // use only name in order to correctly invalidate Spark cache
    SparkView that = (SparkView) other;
    return icebergView.name().equals(that.icebergView.name());
  }

  @Override
  public int hashCode() {
    // use only name in order to correctly invalidate Spark cache
    return icebergView.name().hashCode();
  }
}
