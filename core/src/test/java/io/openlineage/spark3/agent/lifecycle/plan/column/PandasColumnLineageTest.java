/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.spark.api.python.PythonFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.Alias;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.PythonUDF;
import org.apache.spark.sql.catalyst.plans.logical.FlatMapGroupsInArrow;
import org.apache.spark.sql.catalyst.plans.logical.FlatMapGroupsInPandas;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.MapInArrow;
import org.apache.spark.sql.catalyst.plans.logical.MapInPandas;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.types.DataTypeUtils;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import scala.Option;
import scala.collection.immutable.Seq;

/**
 * The pandas operator family — {@code MapInPandas}, {@code MapInArrow}, {@code
 * FlatMapGroupsInPandas}, {@code FlatMapGroupsInArrow} — on a <b>real</b> analyzed Spark 4 plan
 * rather than a mock.
 *
 * <p>Node construction is pure JVM: a {@code PythonUDF} with a mocked {@code PythonFunction} (the
 * trait is {@code private[spark]} in Scala but public in bytecode, so Java can see and Mockito can
 * mock it), wrapped into the four logical operators exactly as PySpark does — UDF children = every
 * column of the child, output = fresh attributes from the UDF's return schema. No Python worker,
 * no pandas import, nothing executed. The plan is analyzed (the visitor sees {@code
 * optimizedPlan}, exactly what a listener would traverse).
 *
 * <p>Measured plan shapes (PySpark 4.0.0, .tmp/pyspark-probe/probe.out):
 *
 * <ul>
 *   <li>mapInPandas: {@code MapInPandas <lambda>(a, b, s), [o1, o2] over LogicalRDD}
 *   <li>applyInPandas groupBy(a): {@code FlatMapGroupsInPandas [a], <lambda>(a, b, s), [o1, o2]
 *       over Project[a, a, b, s] over LogicalRDD} — grouping attributes are <b>real</b> child
 *       attributes (exprIds preserved), unlike the typed groupByKey path which wraps the child in
 *       AppendColumns and needs the deserializer resolution.
 *   <li>expression key: the grouping attribute list carries the Alias ({@code ceil(b/2)}), which
 *       plain traversal resolves through the Alias in the Project below.
 * </ul>
 *
 * <p>Dispatch in isolation ({@code isDefinedAt}/{@code apply} against mocks) is covered by {@code
 * PandasVisitorTest}. Controls for existing pandas-expression behaviour (scalar {@code pandas_udf}
 * in a Project, {@code pandas_udf} agg) are in {@code TypedOperationFixtures} suites.
 *
 * @see TypedLineageTestBase for the harness
 */
class PandasColumnLineageTest extends TypedLineageTestBase {

  // Eval types from PythonRunner/PythonEvalType — the constants the real PySpark nodes carry.
  private static final int SQL_MAP_PANDAS_ITER_UDF = 205;
  private static final int SQL_GROUPED_MAP_PANDAS_UDF = 201;
  private static final int SQL_MAP_ARROW_ITER_UDF = 207;
  private static final int SQL_GROUPED_MAP_ARROW_UDF = 209;

  // ---------------------------------------------------------------------------
  // node construction
  // ---------------------------------------------------------------------------

  /** The three-column RDD-backed child (LogicalRDD [a, b, s]) every case builds on. */
  private LogicalPlan child() {
    return source().queryExecution().logical();
  }

  /** Fresh output attributes [o1, o2] from the UDF's return schema, as PySpark derives them. */
  private static Seq<Attribute> outputAttributes() {
    StructType schema =
        new StructType().add("o1", DataTypes.IntegerType).add("o2", DataTypes.StringType);
    List<Attribute> attrs = new ArrayList<>();
    ScalaConversionUtils.<AttributeReference>fromSeq(DataTypeUtils.toAttributes(schema))
        .forEach(attrs::add);
    return ScalaConversionUtils.fromList(attrs);
  }

  /**
   * A {@code PythonUDF} whose children are every child column — the exact PySpark construction
   * ({@code udf_column = udf(*[df[col] for col in df.columns])}).
   */
  private static PythonUDF pandasUdf(String name, LogicalPlan child, int evalType) {
    StructType returnType =
        new StructType().add("o1", DataTypes.IntegerType).add("o2", DataTypes.StringType);
    List<Expression> children = new ArrayList<>();
    ScalaConversionUtils.<Attribute>fromSeq(child.output()).forEach(children::add);
    return new PythonUDF(
        name,
        mock(PythonFunction.class),
        returnType,
        ScalaConversionUtils.fromList(children),
        evalType,
        true,
        NamedExpression.newExprId());
  }

  /** Wraps a constructed plan in a Dataset so the harness can analyze it. */
  private Dataset<Row> ofRows(LogicalPlan plan) {
    return org.apache.spark.sql.classic.Dataset.ofRows(
        (org.apache.spark.sql.classic.SparkSession) spark(), plan);
  }

  private Dataset<Row> mapInPandas() {
    LogicalPlan child = child();
    PythonUDF udf = pandasUdf("<lambda>", child, SQL_MAP_PANDAS_ITER_UDF);
    return ofRows(new MapInPandas(udf, outputAttributes(), child, false, Option.empty()));
  }

  private Dataset<Row> mapInArrow() {
    LogicalPlan child = child();
    PythonUDF udf = pandasUdf("<lambda>", child, SQL_MAP_ARROW_ITER_UDF);
    return ofRows(new MapInArrow(udf, outputAttributes(), child, false, Option.empty()));
  }

  /**
   * Builds a {@code FlatMapGroupsInPandas}/{@code FlatMapGroupsInArrow} node over a {@code Project}
   * whose list is {@code groupingExprs ++ child.output}, mirroring {@code
   * RelationalGroupedDataset.flatMapGroupsInPandas}: the grouping attributes are {@code
   * project.output.take(groupingExprs.length)}.
   */
  private LogicalPlan flatMapGroups(
      LogicalPlan child, List<NamedExpression> groupingExprs, int evalType, boolean arrow) {
    PythonUDF udf = pandasUdf("<lambda>", child, evalType);
    List<NamedExpression> projectList = new ArrayList<>(groupingExprs);
    ScalaConversionUtils.<Attribute>fromSeq(child.output()).forEach(projectList::add);
    Project project = new Project(ScalaConversionUtils.fromList(projectList), child);
    // Analyze the project before reading its output, mirroring
    // RelationalGroupedDataset.flatMapGroupsInPandas: only after analysis does an Alias grouping
    // key resolve to a real AttributeReference (carrying the Alias's exprId) rather than an
    // UnresolvedAttribute with no exprId to traverse.
    LogicalPlan analyzedProject = ofRows(project).queryExecution().analyzed();
    List<Attribute> groupingAttrs =
        ScalaConversionUtils.<Attribute>fromSeq(analyzedProject.output())
            .subList(0, groupingExprs.size());
    Seq<Attribute> grouping = ScalaConversionUtils.fromList(groupingAttrs);
    if (arrow) {
      return new FlatMapGroupsInArrow(grouping, udf, outputAttributes(), analyzedProject);
    }
    return new FlatMapGroupsInPandas(grouping, udf, outputAttributes(), analyzedProject);
  }

  private Dataset<Row> applyInPandasByColumnKey() {
    LogicalPlan child = child();
    Attribute a = ScalaConversionUtils.<Attribute>fromSeq(child.output()).get(0);
    return ofRows(
        flatMapGroups(child, Collections.singletonList(a), SQL_GROUPED_MAP_PANDAS_UDF, false));
  }

  private Dataset<Row> applyInArrowByColumnKey() {
    LogicalPlan child = child();
    Attribute a = ScalaConversionUtils.<Attribute>fromSeq(child.output()).get(0);
    return ofRows(
        flatMapGroups(child, Collections.singletonList(a), SQL_GROUPED_MAP_ARROW_UDF, true));
  }

  private Dataset<Row> applyInPandasByExpressionKey() {
    LogicalPlan child = child();
    List<Attribute> cols = ScalaConversionUtils.<Attribute>fromSeq(child.output());
    Attribute a = cols.get(0);
    Attribute b = cols.get(1);
    Alias alias =
        new Alias(
            b,
            "b_alias",
            NamedExpression.newExprId(),
            ScalaConversionUtils.asScalaSeqEmpty(),
            Option.empty(),
            ScalaConversionUtils.asScalaSeqEmpty());
    List<NamedExpression> groupingExprs = new ArrayList<>();
    groupingExprs.add(a);
    groupingExprs.add(alias);
    return ofRows(flatMapGroups(child, groupingExprs, SQL_GROUPED_MAP_PANDAS_UDF, false));
  }

  // ---------------------------------------------------------------------------
  // mapInPandas / mapInArrow
  // ---------------------------------------------------------------------------

  @Test
  @DisplayName("mapInPandas records an INDIRECT/TRANSFORMATION fan-in, unflagged")
  void mapInPandasEmitsFanIn() {
    Dataset<Row> ds = mapInPandas();

    assertThat(planChain(ds)).isEqualTo("MapInPandas>LogicalRDD");

    assertThat(edges(ds))
        .containsExactlyInAnyOrder(
            "o1<-a INDIRECT/TRANSFORMATION/mask=false",
            "o1<-b INDIRECT/TRANSFORMATION/mask=false",
            "o1<-s INDIRECT/TRANSFORMATION/mask=false",
            "o2<-a INDIRECT/TRANSFORMATION/mask=false",
            "o2<-b INDIRECT/TRANSFORMATION/mask=false",
            "o2<-s INDIRECT/TRANSFORMATION/mask=false");

    assertThat(facetFields(ds))
        .containsExactlyInAnyOrder(
            "o1<-a INDIRECT/TRANSFORMATION/mask=false",
            "o1<-b INDIRECT/TRANSFORMATION/mask=false",
            "o1<-s INDIRECT/TRANSFORMATION/mask=false",
            "o2<-a INDIRECT/TRANSFORMATION/mask=false",
            "o2<-b INDIRECT/TRANSFORMATION/mask=false",
            "o2<-s INDIRECT/TRANSFORMATION/mask=false");

    // The description carries the UDF name read off the PythonUDF.
    assertThat(describedFacetFields(ds))
        .allMatch(edge -> edge.endsWith("/desc=MapInPandas(<lambda>)"));
  }

  @Test
  @DisplayName("mapInArrow records the same fan-in on the MapInArrow node")
  void mapInArrowEmitsFanIn() {
    Dataset<Row> ds = mapInArrow();

    assertThat(planChain(ds)).isEqualTo("MapInArrow>LogicalRDD");

    assertThat(edges(ds))
        .containsExactlyInAnyOrder(
            "o1<-a INDIRECT/TRANSFORMATION/mask=false",
            "o1<-b INDIRECT/TRANSFORMATION/mask=false",
            "o1<-s INDIRECT/TRANSFORMATION/mask=false",
            "o2<-a INDIRECT/TRANSFORMATION/mask=false",
            "o2<-b INDIRECT/TRANSFORMATION/mask=false",
            "o2<-s INDIRECT/TRANSFORMATION/mask=false");

    assertThat(describedFacetFields(ds))
        .allMatch(edge -> edge.endsWith("/desc=MapInArrow(<lambda>)"));
  }

  // ---------------------------------------------------------------------------
  // applyInPandas / applyInArrow
  // ---------------------------------------------------------------------------

  @Test
  @DisplayName("applyInPandas by a real column key records the fan-in plus a GROUP_BY on the key")
  void applyInPandasByColumnKeyEmitsFanInAndGrouping() {
    Dataset<Row> ds = applyInPandasByColumnKey();

    assertThat(planChain(ds)).isEqualTo("FlatMapGroupsInPandas>Project>LogicalRDD");

    // The grouping attributes are real child attributes, so the GROUP_BY lands on the key column
    // "a" only — not the whole deserialized schema as the typed groupByKey path does.
    List<String> groupByEdges = edges(ds);
    long dependencyId = soleDatasetDependencyId(groupByEdges, "INDIRECT/GROUP_BY");
    assertThat(groupByEdges)
        .containsExactlyInAnyOrder(
            "o1<-a INDIRECT/TRANSFORMATION/mask=false",
            "o1<-b INDIRECT/TRANSFORMATION/mask=false",
            "o1<-s INDIRECT/TRANSFORMATION/mask=false",
            "o2<-a INDIRECT/TRANSFORMATION/mask=false",
            "o2<-b INDIRECT/TRANSFORMATION/mask=false",
            "o2<-s INDIRECT/TRANSFORMATION/mask=false",
            "#" + dependencyId + "<-a INDIRECT/GROUP_BY/mask=false");

    // Facet level: the GROUP_BY dataset dependency rides along on every output field.
    assertThat(facetFields(ds))
        .containsExactlyInAnyOrder(
            "o1<-a INDIRECT/TRANSFORMATION/mask=false",
            "o1<-b INDIRECT/TRANSFORMATION/mask=false",
            "o1<-s INDIRECT/TRANSFORMATION/mask=false",
            "o1<-a INDIRECT/GROUP_BY/mask=false",
            "o2<-a INDIRECT/TRANSFORMATION/mask=false",
            "o2<-b INDIRECT/TRANSFORMATION/mask=false",
            "o2<-s INDIRECT/TRANSFORMATION/mask=false",
            "o2<-a INDIRECT/GROUP_BY/mask=false");
  }

  @Test
  @DisplayName("applyInArrow records the same fan-in plus GROUP_BY on the FlatMapGroupsInArrow node")
  void applyInArrowByColumnKeyEmitsFanInAndGrouping() {
    Dataset<Row> ds = applyInArrowByColumnKey();

    assertThat(planChain(ds)).isEqualTo("FlatMapGroupsInArrow>Project>LogicalRDD");

    List<String> groupByEdges = edges(ds);
    long dependencyId = soleDatasetDependencyId(groupByEdges, "INDIRECT/GROUP_BY");
    assertThat(groupByEdges)
        .containsExactlyInAnyOrder(
            "o1<-a INDIRECT/TRANSFORMATION/mask=false",
            "o1<-b INDIRECT/TRANSFORMATION/mask=false",
            "o1<-s INDIRECT/TRANSFORMATION/mask=false",
            "o2<-a INDIRECT/TRANSFORMATION/mask=false",
            "o2<-b INDIRECT/TRANSFORMATION/mask=false",
            "o2<-s INDIRECT/TRANSFORMATION/mask=false",
            "#" + dependencyId + "<-a INDIRECT/GROUP_BY/mask=false");
  }

  @Test
  @DisplayName("an expression grouping key resolves GROUP_BY through the Alias to the column")
  void applyInPandasByExpressionKeyResolvesGroupingThroughAlias() {
    Dataset<Row> ds = applyInPandasByExpressionKey();

    assertThat(planChain(ds)).isEqualTo("FlatMapGroupsInPandas>Project>LogicalRDD");

    // The grouping is [a, alias(b)]. The GROUP_BY on the alias must resolve through the Alias in
    // the Project below to column b — not stall at the alias's own exprId.
    assertThat(facetFields(ds))
        .filteredOn(edge -> edge.contains("GROUP_BY"))
        .containsExactlyInAnyOrder(
            "o1<-a INDIRECT/GROUP_BY/mask=false",
            "o1<-b INDIRECT/GROUP_BY/mask=false",
            "o2<-a INDIRECT/GROUP_BY/mask=false",
            "o2<-b INDIRECT/GROUP_BY/mask=false");
  }

  // ---------------------------------------------------------------------------
  // gating
  // ---------------------------------------------------------------------------

  @Test
  @DisplayName("the pandas fan-in ships unflagged, unlike the typed boundary")
  void pandasFanInIsNotGatedBehindTheFlag() {
    // defaultConfig() has typedBoundaryFanInEnabled=false. The pandas fan-in still emits, because
    // the claim is exact (Spark marshals exactly the UDF's argument columns) rather than a
    // pessimistic fan-in across an opaque object boundary.
    assertThat(facetFields(mapInPandas())).hasSize(6);
  }
}
