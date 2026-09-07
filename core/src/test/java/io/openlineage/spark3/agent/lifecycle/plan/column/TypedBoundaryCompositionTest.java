/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Composed typed transformations — two or more typed Dataset operations in one plan — and how the
 * fan-in ({@code INDIRECT/TRANSFORMATION}), the typed filter ({@code INDIRECT/FILTER}) and the typed
 * grouping ({@code INDIRECT/GROUP_BY}) edges coexist across them.
 *
 * <p>The single-operation suites ({@link TypedBoundaryColumnLineageTest}, {@link
 * TypedFilterColumnLineageTest}, {@link MapGroupsColumnLineageTest}) each pin one visitor in
 * isolation. This suite pins what happens when they share a plan, which is where the visitors'
 * assumptions about plan shape are actually exercised.
 *
 * <p>Measured on Spark 4.0.0 / Scala 2.13.
 *
 * @see TypedLineageTestBase for the harness
 */
class TypedBoundaryCompositionTest extends TypedLineageTestBase {

  // ---------------------------------------------------------------------------
  // working compositions
  // ---------------------------------------------------------------------------

  @Test
  @DisplayName("map -> mapPartitions: the fan-in fires once and chains both operator names")
  void mapThenMapPartitionsChainsDescriptions() {
    Dataset<Row> ds =
        source()
            .map((MapFunction<Row, Row>) row -> row, Encoders.row(source().schema()))
            .mapPartitions(
                (MapPartitionsFunction<Row, Row>) rows -> rows, Encoders.row(source().schema()));

    // Two typed nodes, one SerializeFromObject boundary: the fan-in fires once, not once per node.
    assertThat(planChain(ds))
        .isEqualTo("SerializeFromObject>MapPartitions>MapElements>DeserializeToObject>LogicalRDD");
    assertThat(edges(ds)).isEmpty();

    List<String> described = describedEdgesWithTypedBoundaryFanIn(ds, null);
    assertThat(described).hasSize(9).allMatch(e -> e.contains("INDIRECT/TRANSFORMATION/mask=false"));
    // The description joins every opaque operator in the chain, in plan order, and the same-type
    // flag rides on the MapElements hop (Row -> Row) while MapPartitions carries no argument type.
    assertThat(descriptionOf(described))
        .isEqualTo("MapPartitions(-> Row), MapElements(Row -> Row), sameType");

    assertThat(facetFieldsWithTypedBoundaryFanIn(ds)).hasSize(9);
  }

  @Test
  @DisplayName("map -> flatMap: flatMap lowers to MapPartitions, so the same chain holds")
  void mapThenFlatMapChainsDescriptions() {
    Dataset<Row> ds =
        source()
            .map((MapFunction<Row, Row>) row -> row, Encoders.row(source().schema()))
            .flatMap(
                (FlatMapFunction<Row, Row>) row -> Collections.singletonList(row).iterator(),
                Encoders.row(source().schema()));

    assertThat(planChain(ds))
        .isEqualTo("SerializeFromObject>MapPartitions>MapElements>DeserializeToObject>LogicalRDD");

    List<String> described = describedEdgesWithTypedBoundaryFanIn(ds, null);
    assertThat(described).hasSize(9);
    assertThat(descriptionOf(described))
        .isEqualTo("MapPartitions(-> Row), MapElements(Row -> Row), sameType");
  }

  // ---------------------------------------------------------------------------
  // previously-silent compositions (fixed: the fan-in now descends through Filter)
  // ---------------------------------------------------------------------------

  @Test
  @DisplayName("map -> filter: the fan-in descends through the Filter and fires")
  void filterBetweenBoundaryAndTypedNodeNoLongerSilencesFanIn() {
    // ds.map(identity).filter(predicate): the Filter sits between SerializeFromObject and
    // MapElements. collectInputs now descends through it to DeserializeToObject, so the fan-in
    // fires and the filter signal rides alongside it.
    Dataset<Row> ds =
        source()
            .map((MapFunction<Row, Row>) row -> row, Encoders.row(source().schema()))
            .filter((FilterFunction<Row>) row -> row.getInt(0) > 1);

    assertThat(planChain(ds))
        .isEqualTo("SerializeFromObject>Filter>MapElements>DeserializeToObject>LogicalRDD");

    List<String> described = describedEdgesWithTypedBoundaryFanIn(ds, null);
    assertThat(described)
        .anyMatch(e -> e.contains("INDIRECT/TRANSFORMATION"))
        .anyMatch(e -> e.contains("INDIRECT/FILTER"));
    assertThat(facetFieldsWithTypedBoundaryFanIn(ds)).isNotEmpty();
  }

  @Test
  @DisplayName("filter -> map: the fan-in descends through the Filter and fires")
  void filterBelowTypedNodeNoLongerSilencesFanIn() {
    // ds.filter(predicate).map(identity): the Filter sits between MapElements and
    // DeserializeToObject. collectInputs now descends through it, so the fan-in fires.
    Dataset<Row> ds =
        source()
            .filter((FilterFunction<Row>) row -> row.getInt(0) > 1)
            .map((MapFunction<Row, Row>) row -> row, Encoders.row(source().schema()));

    assertThat(planChain(ds))
        .isEqualTo("SerializeFromObject>MapElements>Filter>DeserializeToObject>LogicalRDD");

    List<String> described = describedEdgesWithTypedBoundaryFanIn(ds, null);
    assertThat(described)
        .anyMatch(e -> e.contains("INDIRECT/TRANSFORMATION"))
        .anyMatch(e -> e.contains("INDIRECT/FILTER"));
    assertThat(facetFieldsWithTypedBoundaryFanIn(ds)).isNotEmpty();
  }

  @Test
  @DisplayName("map -> groupByKey().mapGroups: the grouping key resolves through AppendColumnsWithObject")
  void mapThenGroupByResolvesGroupingKeyThroughAppendColumnsWithObject() {
    // ds.map(identity).groupByKey(...).mapGroups(...): the map makes the intermediate object-typed,
    // so the grouping's AppendColumns becomes AppendColumnsWithObject. TypedGroupByVisitor now
    // resolves the key through the value deserializer, which references the real columns a/b/s.
    Dataset<Integer> ds =
        source()
            .map((MapFunction<Row, Row>) row -> row, Encoders.row(source().schema()))
            .groupByKey((MapFunction<Row, Integer>) row -> row.getInt(0) % 2, Encoders.INT())
            .mapGroups(
                (MapGroupsFunction<Integer, Row, Integer>) (key, rows) -> key, Encoders.INT());

    assertThat(planChain(ds))
        .isEqualTo(
            "SerializeFromObject>MapGroups>AppendColumnsWithObject>MapElements>DeserializeToObject>LogicalRDD");

    // The GROUP_BY edge now resolves to the real columns a/b/s, not the synthetic key `value`.
    List<String> described = describedEdgesWithTypedBoundaryFanIn(ds, null);
    assertThat(described)
        .anyMatch(e -> e.contains("INDIRECT/GROUP_BY") && e.endsWith("<-a INDIRECT/GROUP_BY/mask=false/desc="))
        .anyMatch(e -> e.contains("INDIRECT/GROUP_BY") && e.endsWith("<-b INDIRECT/GROUP_BY/mask=false/desc="))
        .anyMatch(e -> e.contains("INDIRECT/GROUP_BY") && e.endsWith("<-s INDIRECT/GROUP_BY/mask=false/desc="));
    assertThat(facetFieldsWithTypedBoundaryFanIn(ds)).isNotEmpty();
  }

  private static String descriptionOf(List<String> described) {
    return described.get(0).substring(described.get(0).indexOf("/desc=") + "/desc=".length());
  }
}
