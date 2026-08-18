/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.Alias;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.PythonUDF;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.IntegerType$;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import scala.Option;

/**
 * UDF column lineage, plus the untyped controls that make the harness itself trustworthy.
 *
 * <p>A user-defined function must not be reported with the same confidence as an expression whose
 * semantics Spark actually knows. Before {@code UserDefinedExpressionVisitor} every {@code
 * UserDefinedExpression} fell through {@code ExpressionTraverser}'s generic {@code children()}
 * fallback and produced a {@code DIRECT/TRANSFORMATION} edge per argument — byte-identical to plain
 * {@code a + b}, so a consumer could not tell {@code upper(name)} from {@code myUdf(name)}. The
 * defect was not a missing edge but a falsely confident one; the visitor now emits {@code
 * INDIRECT/TRANSFORMATION} carrying the function name in the description.
 *
 * <p>The controls live here rather than in a class of their own because every other suite in this
 * family reads them as the reference for what a well-understood expression looks like. They pin plain
 * identity, arithmetic, masking and untyped {@code filter(Column)}; a control that drifts silently
 * invalidates every comparison the other suites make against it.
 *
 * @see TypedLineageTestBase for the harness
 */
class UdfColumnLineageTest extends TypedLineageTestBase {

  // ===========================================================================
  // controls — untyped expressions, so the harness itself is trustworthy
  // ===========================================================================

  @Test
  @DisplayName("CONTROL: a bare column reference is DIRECT/IDENTITY")
  void controlIdentityProjection() {
    Dataset<Row> ds = source().select(functions.col("a").as("out"));

    assertThat(planChain(ds)).isEqualTo("Project>LogicalRDD");
    assertThat(edges(ds)).containsExactly("out<-a DIRECT/IDENTITY/mask=false");
    assertThat(facetFields(ds)).containsExactly("out<-a DIRECT/IDENTITY/mask=false");
  }

  @Test
  @DisplayName("CONTROL: arithmetic over two columns fans in as DIRECT/TRANSFORMATION")
  void controlArithmeticProjection() {
    Dataset<Row> ds = source().select(functions.col("a").plus(functions.col("b")).as("out"));

    assertThat(planChain(ds)).isEqualTo("Project>LogicalRDD");
    assertThat(edges(ds))
        .containsExactlyInAnyOrder(
            "out<-a DIRECT/TRANSFORMATION/mask=false", "out<-b DIRECT/TRANSFORMATION/mask=false");
    assertThat(facetFields(ds))
        .containsExactlyInAnyOrder(
            "out<-a DIRECT/TRANSFORMATION/mask=false", "out<-b DIRECT/TRANSFORMATION/mask=false");
  }

  @Test
  @DisplayName("CONTROL: sha1 is recognised as masking, so mask=true is reachable")
  void controlMaskingExpression() {
    Dataset<Row> ds = source().select(functions.sha1(functions.col("s")).as("out"));

    assertThat(edges(ds)).containsExactly("out<-s DIRECT/TRANSFORMATION/mask=true");
    assertThat(facetFields(ds)).containsExactly("out<-s DIRECT/TRANSFORMATION/mask=true");
  }

  @Test
  @DisplayName("CONTROL: untyped filter(Column) contributes an INDIRECT/FILTER edge")
  void controlUntypedFilterIsIndirect() {
    Dataset<Row> ds =
        source().filter(functions.col("a").gt(1)).select(functions.col("a").as("out"));

    assertThat(planChain(ds)).isEqualTo("Project>Filter>LogicalRDD");
    assertThat(edges(ds))
        .contains("out<-a DIRECT/IDENTITY/mask=false")
        .anyMatch(edge -> edge.endsWith("<-a INDIRECT/FILTER/mask=false"));
    assertThat(facetFields(ds))
        .containsExactlyInAnyOrder(
            "out<-a DIRECT/IDENTITY/mask=false", "out<-a INDIRECT/FILTER/mask=false");
  }

  // ===========================================================================
  // ScalaUDF — edges exist AND are marked semantically opaque. The fan-in is unchanged from the
  // DIRECT/TRANSFORMATION behaviour these assertions replaced; only its honesty improved.
  // ===========================================================================

  @Test
  @DisplayName("ScalaUDF, 1 arg: edge is INDIRECT/TRANSFORMATION and names the function")
  void scalaUdfSingleArgumentEmitsOpaqueTransformationEdge() {
    Dataset<Row> ds = source().select(functions.callUDF("plus1", functions.col("a")).as("out"));

    assertThat(planChain(ds)).isEqualTo("Project>LogicalRDD");
    assertThat(edges(ds)).containsExactly("out<-a INDIRECT/TRANSFORMATION/mask=false");
    assertThat(facetFields(ds)).containsExactly("out<-a INDIRECT/TRANSFORMATION/mask=false");

    // The marker is not merely internal: the function identity survives into the facet, which is
    // the only observation level a consumer actually sees.
    assertThat(describedEdges(ds))
        .containsExactly("out<-a INDIRECT/TRANSFORMATION/mask=false/desc=UDF: plus1");
    assertThat(describedFacetFields(ds))
        .containsExactly("out<-a INDIRECT/TRANSFORMATION/mask=false/desc=UDF: plus1");
  }

  @Test
  @DisplayName("ScalaUDF, 2 args: fan-in preserved, one INDIRECT/TRANSFORMATION edge per argument")
  void scalaUdfMultipleArgumentsFanInFromEveryArgument() {
    Dataset<Row> ds =
        source()
            .select(functions.callUDF("addTwo", functions.col("a"), functions.col("b")).as("out"));

    // Both arguments still contribute: marking opacity must not cost us the fan-in.
    assertThat(edges(ds))
        .containsExactlyInAnyOrder(
            "out<-a INDIRECT/TRANSFORMATION/mask=false",
            "out<-b INDIRECT/TRANSFORMATION/mask=false");
    assertThat(facetFields(ds))
        .containsExactlyInAnyOrder(
            "out<-a INDIRECT/TRANSFORMATION/mask=false",
            "out<-b INDIRECT/TRANSFORMATION/mask=false");
    assertThat(describedEdges(ds))
        .containsExactlyInAnyOrder(
            "out<-a INDIRECT/TRANSFORMATION/mask=false/desc=UDF: addTwo",
            "out<-b INDIRECT/TRANSFORMATION/mask=false/desc=UDF: addTwo");
  }

  @Test
  @DisplayName("ScalaUDF nested in ScalaUDF: recursion still flattens, opacity is not lost")
  void scalaUdfNestedCallFlattensToTheSameEdgeSet() {
    Dataset<Row> ds =
        source()
            .select(
                functions
                    .callUDF(
                        "plus1", functions.callUDF("addTwo", functions.col("a"), functions.col("b")))
                    .as("out"));

    assertThat(edges(ds))
        .containsExactlyInAnyOrder(
            "out<-a INDIRECT/TRANSFORMATION/mask=false",
            "out<-b INDIRECT/TRANSFORMATION/mask=false");

    // Nesting depth is still not recorded, and the description reports the OUTERMOST function:
    // TransformationInfo.merge keeps the existing INDIRECT info (rule 1) rather than the inner
    // one, so `plus1` wins over `addTwo`. Pinned deliberately -- naming one of the two functions
    // is the ceiling of a single description field, and a follow-up that wants the full chain will
    // have to change the encoding, which should show up as a failure here.
    assertThat(describedEdges(ds))
        .containsExactlyInAnyOrder(
            "out<-a INDIRECT/TRANSFORMATION/mask=false/desc=UDF: plus1",
            "out<-b INDIRECT/TRANSFORMATION/mask=false/desc=UDF: plus1");
  }

  @Test
  @DisplayName("FIXED: a UDF is now distinguishable from equivalent plain arithmetic")
  void udfEdgesAreDistinguishableFromPlainArithmetic() {
    List<String> arithmetic =
        edges(source().select(functions.col("a").plus(functions.col("b")).as("out")));
    List<String> flatUdf =
        edges(
            source()
                .select(
                    functions.callUDF("addTwo", functions.col("a"), functions.col("b")).as("out")));

    // `addTwo(a, b)` and `a + b` connect the same fields, but a consumer can now tell that opaque
    // user code sat in between.
    //
    // Asserted as whole edge strings, NOT with contains("DIRECT/TRANSFORMATION"): "INDIRECT/
    // TRANSFORMATION" contains "DIRECT/TRANSFORMATION" as a substring, so a substring check would
    // still pass if the arithmetic control silently regressed to INDIRECT.
    assertThat(flatUdf).isNotEqualTo(arithmetic);
    assertThat(arithmetic)
        .containsExactlyInAnyOrder(
            "out<-a DIRECT/TRANSFORMATION/mask=false", "out<-b DIRECT/TRANSFORMATION/mask=false");
    assertThat(flatUdf)
        .containsExactlyInAnyOrder(
            "out<-a INDIRECT/TRANSFORMATION/mask=false",
            "out<-b INDIRECT/TRANSFORMATION/mask=false");
  }

  @Test
  @DisplayName("built-ins are unaffected: upper()/concat() keep DIRECT typing and empty description")
  void builtInScalarFunctionsAreNotMarkedOpaque() {
    // The narrow-blast-radius claim, asserted rather than assumed: a genuinely known expression
    // must not be caught by the UserDefinedExpression visitor.
    assertThat(describedEdges(source().select(functions.upper(functions.col("s")).as("out"))))
        .containsExactly("out<-s DIRECT/TRANSFORMATION/mask=false/desc=");

    // concat(s, s) fans in twice from the same column, so assert the exact edge set rather than a
    // startsWith prefix ending in "desc=" -- that prefix would match any non-empty description too,
    // and an empty description is exactly what proves the visitor did not fire.
    assertThat(
            describedEdges(
                source()
                    .select(functions.concat(functions.col("s"), functions.col("s")).as("out"))))
        .containsOnly("out<-s DIRECT/TRANSFORMATION/mask=false/desc=");
  }

  @Test
  @DisplayName("masking of a KNOWN inner expression still propagates through an opaque UDF")
  void maskingPropagatesThroughUdfWhenInnerExpressionIsKnownMasking() {
    // plus1(sha1(s)): the UDF itself cannot claim masking, but sha1 can, and TransformationInfo
    // .merge must carry that through. Guards against the opacity marker swallowing a real signal.
    Dataset<Row> ds =
        source().select(functions.callUDF("plus1", functions.sha1(functions.col("s"))).as("out"));

    assertThat(edges(ds)).containsExactly("out<-s INDIRECT/TRANSFORMATION/mask=true");
  }

  @Test
  @DisplayName("an opaque UDF over a sensitive column is still mask=false — unknowable, not false")
  void udfOverSensitiveColumnIsNotReportedAsMasking() {
    // Whether a UDF obfuscates its input cannot be determined, and claiming mask=true would be as
    // wrong as the DIRECT edge that was removed.
    // The control test controlMaskingExpression proves mask=true IS reachable, so this is a real
    // negative rather than a blind spot.
    assertThat(edges(source().select(functions.callUDF("plus1", functions.col("a")).as("out"))))
        .allMatch(edge -> edge.endsWith("mask=false"));
  }

  // ===========================================================================
  // PythonUDF — must get the same treatment, or Scala and Python UDFs silently disagree.
  // ===========================================================================

  @Test
  @DisplayName(
      "PythonUDF is marked opaque too, so Scala and Python UDFs do not disagree for no reason")
  void pythonUdfExposesChildrenAndIsMarkedOpaqueLikeScalaUdf() {
    AttributeReference a = attribute("a", 101);
    AttributeReference b = attribute("b", 102);

    PythonUDF pythonUdf =
        new PythonUDF(
            "pyAddTwo",
            mock(org.apache.spark.api.python.PythonFunction.class),
            IntegerType$.MODULE$,
            ScalaConversionUtils.fromList(Arrays.asList((Expression) a, (Expression) b)),
            100, // PythonEvalType.SQL_BATCHED_UDF
            true,
            ExprId.apply(200));

    // PythonUDF arguments are ordinary children, not opaque state, so the same visitor path applies.
    assertThat(pythonUdf.children().size()).isEqualTo(2);
    assertThat(ScalaConversionUtils.fromSeq(pythonUdf.children())).containsExactly(a, b);

    Project project =
        new Project(
            ScalaConversionUtils.fromList(
                Collections.<NamedExpression>singletonList(
                    new Alias(
                        pythonUdf,
                        "out",
                        ExprId.apply(300),
                        ScalaConversionUtils.asScalaSeqEmpty(),
                        Option.empty(),
                        ScalaConversionUtils.asScalaSeqEmpty()))),
            mock(LogicalPlan.class));

    Map<Long, String> names = new HashMap<>();
    names.put(101L, "a");
    names.put(102L, "b");
    names.put(300L, "out");

    // Same fan-in as ScalaUDF, and now the same opacity marker. The Python function name is carried
    // through the same path even though PythonUDF exposes it as `name` rather than `udfName`.
    assertThat(edges(project, names))
        .containsExactlyInAnyOrder(
            "out<-a INDIRECT/TRANSFORMATION/mask=false",
            "out<-b INDIRECT/TRANSFORMATION/mask=false");
    assertThat(describedEdges(project, names))
        .containsExactlyInAnyOrder(
            "out<-a INDIRECT/TRANSFORMATION/mask=false/desc=UDF: pyAddTwo",
            "out<-b INDIRECT/TRANSFORMATION/mask=false/desc=UDF: pyAddTwo");
  }
}
