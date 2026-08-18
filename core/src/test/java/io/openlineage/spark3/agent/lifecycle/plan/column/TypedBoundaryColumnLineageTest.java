/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.Collections;
import java.util.List;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The {@code Serialize}/{@code Deserialize} typed boundary — {@code Dataset.map}, {@code
 * mapPartitions}, {@code flatMap} — and the opt-in pessimistic fan-in across it.
 *
 * <p>One {@code TypedBoundaryFanInVisitor} matched on {@code SerializeFromObject} emits every output ×
 * every input as {@code INDIRECT/TRANSFORMATION}, gated behind {@code
 * ColumnLineageConfig.typedBoundaryFanInEnabled}, which ships {@code false}. See {@code
 * docs/design/typed-boundary-emission-policy.md} for the alternatives that were measured and rejected.
 *
 * <p><b>The flag-off/flag-on pairing in every test here is load-bearing, not redundant.</b> Flag off
 * pins the historical silence, which must not regress for users who never opt in; flag on pins the
 * fan-in. A test asserting only one would let the other drift unnoticed — and since the fan-in ships
 * dark, the flag-off half is what protects every existing user.
 *
 * @see TypedLineageTestBase for the harness
 */
class TypedBoundaryColumnLineageTest extends TypedLineageTestBase {

  @Test
  @DisplayName("Dataset.map (Java encoder): silent with the flag off, fans in with it on")
  void typedMapJavaEncoder() {
    Dataset<Row> ds =
        source().map((MapFunction<Row, Row>) row -> row, Encoders.row(source().schema()));

    assertThat(planChain(ds))
        .isEqualTo("SerializeFromObject>MapElements>DeserializeToObject>LogicalRDD");

    // Flag off: unchanged from the pre-fix baseline.
    assertThat(edges(ds)).isEmpty();
    assertThat(facetFields(ds)).isEmpty();

    // Flag on: every output field depends on every input field, INDIRECT and named opaque. Three
    // columns in, three out, so 9 edges. Not an identity claim, which the plan cannot support.
    assertThat(edgesWithTypedBoundaryFanIn(ds, null))
        .hasSize(9)
        .allMatch(edge -> edge.contains("INDIRECT/TRANSFORMATION/mask=false"))
        .contains(
            "a<-a INDIRECT/TRANSFORMATION/mask=false",
            "a<-b INDIRECT/TRANSFORMATION/mask=false",
            "a<-s INDIRECT/TRANSFORMATION/mask=false",
            "b<-a INDIRECT/TRANSFORMATION/mask=false",
            "s<-a INDIRECT/TRANSFORMATION/mask=false");
    assertThat(facetFieldsWithTypedBoundaryFanIn(ds))
        .hasSize(9)
        .allMatch(field -> field.endsWith("INDIRECT/TRANSFORMATION/mask=false"));
  }

  @Test
  @DisplayName("Dataset.map (Scala encoder): encoder flavour is irrelevant to the fan-in")
  void typedMapScalaEncoder() {
    scala.Function1<Long, Object> increment =
        ScalaConversionUtils.toScalaFn((Long value) -> (Object) (value + 1L));
    Dataset<Object> ds = spark().range(0, 3).map(increment, Encoders.scalaLong());

    assertThat(planChain(ds)).isEqualTo("SerializeFromObject>MapElements>DeserializeToObject>Range");
    assertThat(edges(ds)).isEmpty();

    // Range has one column and the output has one, so 1x1 - and the single output attribute is a
    // fresh exprId, so a self-edge is not what is filtered here.
    assertThat(edgesWithTypedBoundaryFanIn(ds, null))
        .hasSize(1)
        .allMatch(edge -> edge.contains("INDIRECT/TRANSFORMATION/mask=false"));
  }

  @Test
  @DisplayName("Dataset.mapPartitions: silent with the flag off, fans in with it on")
  void typedMapPartitions() {
    Dataset<Row> ds =
        source()
            .mapPartitions(
                (MapPartitionsFunction<Row, Row>) rows -> rows, Encoders.row(source().schema()));

    assertThat(planChain(ds))
        .isEqualTo("SerializeFromObject>MapPartitions>DeserializeToObject>LogicalRDD");
    assertThat(edges(ds)).isEmpty();
    assertThat(facetFields(ds)).isEmpty();

    assertThat(edgesWithTypedBoundaryFanIn(ds, null))
        .hasSize(9)
        .allMatch(edge -> edge.contains("INDIRECT/TRANSFORMATION/mask=false"));
    assertThat(facetFieldsWithTypedBoundaryFanIn(ds)).hasSize(9);
  }

  @Test
  @DisplayName("Dataset.flatMap lowers to MapPartitions, so one visitor covers both")
  void typedFlatMapIsCoveredByTheMapPartitionsPath() {
    Dataset<Row> ds =
        source()
            .flatMap(
                (FlatMapFunction<Row, Row>) row -> Collections.singletonList(row).iterator(),
                Encoders.row(source().schema()));

    // flatMap has no plan node of its own - a dedicated visitor would be dead code.
    assertThat(planChain(ds))
        .isEqualTo("SerializeFromObject>MapPartitions>DeserializeToObject>LogicalRDD");
    assertThat(edges(ds)).isEmpty();

    assertThat(edgesWithTypedBoundaryFanIn(ds, null))
        .hasSize(9)
        .allMatch(edge -> edge.contains("INDIRECT/TRANSFORMATION/mask=false"));
  }

  @Test
  @DisplayName("a downstream untyped Project is bridged across the typed hop with the flag on")
  void projectDownstreamOfTypedMapIsBridged() {
    Dataset<Row> ds =
        source()
            .map((MapFunction<Row, Row>) row -> row, Encoders.row(source().schema()))
            .select(functions.col("a").as("out"));

    assertThat(planChain(ds))
        .isEqualTo("Project>SerializeFromObject>MapElements>DeserializeToObject>LogicalRDD");

    // Flag off: the Project's own identity edge dead-ends, because nothing connects
    // SerializeFromObject's output attributes back to the leaf. This is the measured pathology the
    // fan-in exists to address, and it is what "silence" actually looks like to a consumer: not an
    // empty plan, but an edge that goes nowhere.
    assertThat(edges(ds)).anyMatch(edge -> edge.startsWith("out<-"));
    assertThat(facetFields(ds)).isEmpty();

    // Flag on: the boundary supplies the missing output-attr -> leaf-attr links, so the facet is
    // populated. The Project contributes DIRECT/IDENTITY, but merge() lets the INDIRECT edge win,
    // so no field is advertised as a traceable identity across the opaque lambda.
    assertThat(facetFieldsWithTypedBoundaryFanIn(ds))
        .containsExactlyInAnyOrder(
            "out<-a INDIRECT/TRANSFORMATION/mask=false",
            "out<-b INDIRECT/TRANSFORMATION/mask=false",
            "out<-s INDIRECT/TRANSFORMATION/mask=false");
  }

  @Test
  @DisplayName("width cap is a deliberate no-emit, not a truncation")
  void typedBoundaryFanInWidthCapEmitsNothing() {
    Dataset<Row> ds =
        source().map((MapFunction<Row, Row>) row -> row, Encoders.row(source().schema()));

    // 3x3 = 9 edges. A cap of 9 still emits; a cap of 8 emits NOTHING rather than 8 of the 9.
    assertThat(edgesWithTypedBoundaryFanIn(ds, 9)).hasSize(9);
    assertThat(edgesWithTypedBoundaryFanIn(ds, 8)).isEmpty();
    assertThat(edgesWithTypedBoundaryFanIn(ds, 0)).isEmpty();

    // And the facet is empty rather than partially populated, which is the point: a fan-in that
    // would trip ColumnLevelLineageBuilder's own RETURNED_INPUT_FIELD_LIMIT collapses the whole
    // facet for every field, so not emitting it keeps unrelated lineage intact.
    SparkOpenLineageConfig capped = typedBoundaryFanInConfig(8);
    assertThat(facetFields(ds, capped)).isEmpty();
  }

  @Test
  @DisplayName("the fan-in is opaque by construction: identity and field-swapping maps agree")
  void identityAndReorderingMapsAreIndistinguishable() {
    // The whole reason option 3 (encoder-schema name matching) was rejected: these two lambdas mean
    // different things and expose identical plan structure. The fan-in makes the same claim for
    // both, which is honest; a name-matched "a<-a" edge would be a lie for the second.
    Dataset<Row> identity =
        source().map((MapFunction<Row, Row>) row -> row, Encoders.row(source().schema()));
    Dataset<Row> swapped =
        source()
            .map(
                (MapFunction<Row, Row>)
                    row -> RowFactory.create(row.getInt(1), row.getInt(0), row.getString(2)),
                Encoders.row(source().schema()));

    List<String> identityEdges = edgesWithTypedBoundaryFanIn(identity, null);
    List<String> swappedEdges = edgesWithTypedBoundaryFanIn(swapped, null);

    assertThat(swappedEdges).containsExactlyInAnyOrderElementsOf(identityEdges);
    assertThat(identityEdges).allMatch(edge -> edge.contains("INDIRECT/TRANSFORMATION"));
  }

  @Test
  @DisplayName("the fan-in defaults to off, so nothing changes without opting in")
  void typedBoundaryFanInIsOffByDefault() {
    assertThat(new SparkOpenLineageConfig().getColumnLineageConfig().getTypedBoundaryFanInEnabled())
        .isFalse();
    assertThat(
            new SparkOpenLineageConfig().getColumnLineageConfig().getTypedBoundaryFanInMaxEdges())
        .isEqualTo(10_000);
  }
}
