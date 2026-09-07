/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

import static io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.TypedOperationFixtures.EXPR_ID_1;
import static io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.TypedOperationFixtures.EXPR_ID_2;
import static io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.TypedOperationFixtures.EXPR_ID_3;
import static io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.TypedOperationFixtures.EXPR_ID_4;
import static io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.TypedOperationFixtures.NAME_1;
import static io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.TypedOperationFixtures.NAME_2;
import static io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.TypedOperationFixtures.NAME_3;
import static io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.TypedOperationFixtures.asSeq;
import static io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.TypedOperationFixtures.field;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.api.ColumnLineageConfig;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeSet;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.FlatMapGroupsInArrow;
import org.apache.spark.sql.catalyst.plans.logical.FlatMapGroupsInPandas;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.MapInArrow;
import org.apache.spark.sql.catalyst.plans.logical.MapInPandas;
import org.apache.spark.sql.catalyst.plans.logical.MapPartitions;
import org.apache.spark.sql.catalyst.plans.logical.SerializeFromObject;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Isolation-level unit tests for {@link PandasOperatorVisitor}, mirroring the pattern of {@link
 * TransformWithStateVisitorTest}: Catalyst nodes are Mockito mocks, the builder is a Mockito mock,
 * and fixtures are real {@link Attribute} objects from {@link TypedOperationFixtures}.
 *
 * <p>Plan-level behaviour — real Spark 4 session, real pandas plans, edge and facet assertions —
 * would live in a {@code PandasColumnLineageTest} (cf. {@code TransformWithStateColumnLineageTest}).
 */
class PandasVisitorTest {

  private final PandasOperatorVisitor visitor = new PandasOperatorVisitor();
  private final ColumnLevelLineageBuilder builder = mock(ColumnLevelLineageBuilder.class);

  private static final Attribute IN_1 = field(NAME_1, EXPR_ID_1);
  private static final Attribute IN_2 = field(NAME_2, EXPR_ID_2);
  private static final Attribute OUT_1 = field(NAME_3, EXPR_ID_3);
  private static final Attribute OUT_2 =
      field("name4", EXPR_ID_4);

  // ---- isDefinedAt ----

  @Test
  void isDefinedAtMapInPandas() {
    assertTrue(visitor.isDefinedAt(mock(MapInPandas.class)));
  }

  @Test
  void isDefinedAtMapInArrow() {
    assertTrue(visitor.isDefinedAt(mock(MapInArrow.class)));
  }

  @Test
  void isDefinedAtFlatMapGroupsInPandas() {
    assertTrue(visitor.isDefinedAt(mock(FlatMapGroupsInPandas.class)));
  }

  @Test
  void isDefinedAtFlatMapGroupsInArrow() {
    assertTrue(visitor.isDefinedAt(mock(FlatMapGroupsInArrow.class)));
  }

  @Test
  void notDefinedAtMapPartitions() {
    assertFalse(visitor.isDefinedAt(mock(MapPartitions.class)));
  }

  @Test
  void notDefinedAtSerializeFromObject() {
    assertFalse(visitor.isDefinedAt(mock(SerializeFromObject.class)));
  }

  @Test
  void notDefinedAtArbitraryPlan() {
    assertFalse(visitor.isDefinedAt(mock(LogicalPlan.class)));
  }

  // ---- apply: fan-in ----

  @Test
  void mapInPandasEmitsFanInEdges() {
    Expression functionExpr = mockFunctionExpr(IN_1, IN_2);
    MapInPandas node = mock(MapInPandas.class);
    when(node.functionExpr()).thenReturn(functionExpr);
    when(node.output()).thenReturn(asSeq(OUT_1, OUT_2));

    visitor.apply(node, builder);

    TransformationInfo expected = indirectTransformation("MapInPandas");
    // 2 outputs x 2 inputs = 4 edges, no self-edges (different exprIds)
    verify(builder, times(1)).addDependency(eq(EXPR_ID_3), eq(EXPR_ID_1), eq(expected));
    verify(builder, times(1)).addDependency(eq(EXPR_ID_3), eq(EXPR_ID_2), eq(expected));
    verify(builder, times(1)).addDependency(eq(EXPR_ID_4), eq(EXPR_ID_1), eq(expected));
    verify(builder, times(1)).addDependency(eq(EXPR_ID_4), eq(EXPR_ID_2), eq(expected));
    // MapInPandas has no grouping
    verify(builder, never()).addDatasetDependency(any());
  }

  @Test
  void mapInArrowEmitsFanInEdges() {
    Expression functionExpr = mockFunctionExpr(IN_1, IN_2);
    MapInArrow node = mock(MapInArrow.class);
    when(node.functionExpr()).thenReturn(functionExpr);
    when(node.output()).thenReturn(asSeq(OUT_1, OUT_2));

    visitor.apply(node, builder);

    TransformationInfo expected = indirectTransformation("MapInArrow");
    verify(builder, times(1)).addDependency(eq(EXPR_ID_3), eq(EXPR_ID_1), eq(expected));
    verify(builder, times(1)).addDependency(eq(EXPR_ID_3), eq(EXPR_ID_2), eq(expected));
    verify(builder, times(1)).addDependency(eq(EXPR_ID_4), eq(EXPR_ID_1), eq(expected));
    verify(builder, times(1)).addDependency(eq(EXPR_ID_4), eq(EXPR_ID_2), eq(expected));
    verify(builder, never()).addDatasetDependency(any());
  }

  @Test
  void flatMapGroupsInPandasEmitsFanInAndGrouping() {
    Expression functionExpr = mockFunctionExpr(IN_1, IN_2);
    FlatMapGroupsInPandas node = mock(FlatMapGroupsInPandas.class);
    when(node.functionExpr()).thenReturn(functionExpr);
    when(node.output()).thenReturn(asSeq(OUT_1, OUT_2));
    when(node.groupingAttributes()).thenReturn(asSeq(IN_1));

    visitor.apply(node, builder);

    TransformationInfo expected = indirectTransformation("FlatMapGroupsInPandas");
    // fan-in: 2 outputs x 2 inputs = 4 edges
    verify(builder, times(1)).addDependency(eq(EXPR_ID_3), eq(EXPR_ID_1), eq(expected));
    verify(builder, times(1)).addDependency(eq(EXPR_ID_3), eq(EXPR_ID_2), eq(expected));
    verify(builder, times(1)).addDependency(eq(EXPR_ID_4), eq(EXPR_ID_1), eq(expected));
    verify(builder, times(1)).addDependency(eq(EXPR_ID_4), eq(EXPR_ID_2), eq(expected));
    // grouping: dataset dependency minted, then IN_1 traversed as GROUP_BY
    ArgumentCaptor<ExprId> datasetDependency = ArgumentCaptor.forClass(ExprId.class);
    verify(builder, times(1)).addDatasetDependency(datasetDependency.capture());
    verify(builder, times(1))
        .addDependency(
            eq(datasetDependency.getValue()),
            eq(EXPR_ID_1),
            eq(TransformationInfo.indirect(TransformationInfo.Subtypes.GROUP_BY)));
  }

  @Test
  void flatMapGroupsInArrowEmitsFanInAndGrouping() {
    Expression functionExpr = mockFunctionExpr(IN_1, IN_2);
    FlatMapGroupsInArrow node = mock(FlatMapGroupsInArrow.class);
    when(node.functionExpr()).thenReturn(functionExpr);
    when(node.output()).thenReturn(asSeq(OUT_1, OUT_2));
    when(node.groupingAttributes()).thenReturn(asSeq(IN_1));

    visitor.apply(node, builder);

    TransformationInfo expected = indirectTransformation("FlatMapGroupsInArrow");
    verify(builder, times(1)).addDependency(eq(EXPR_ID_3), eq(EXPR_ID_1), eq(expected));
    verify(builder, times(1)).addDependency(eq(EXPR_ID_3), eq(EXPR_ID_2), eq(expected));
    verify(builder, times(1)).addDependency(eq(EXPR_ID_4), eq(EXPR_ID_1), eq(expected));
    verify(builder, times(1)).addDependency(eq(EXPR_ID_4), eq(EXPR_ID_2), eq(expected));
    ArgumentCaptor<ExprId> datasetDependency = ArgumentCaptor.forClass(ExprId.class);
    verify(builder, times(1)).addDatasetDependency(datasetDependency.capture());
    verify(builder, times(1))
        .addDependency(
            eq(datasetDependency.getValue()),
            eq(EXPR_ID_1),
            eq(TransformationInfo.indirect(TransformationInfo.Subtypes.GROUP_BY)));
  }

  // ---- apply: fan-in cap boundary ----

  @Test
  void fanInAboveCapEmitsNothing() {
    OpenLineageContext olContext = mock(OpenLineageContext.class);
    SparkOpenLineageConfig olConfig = mock(SparkOpenLineageConfig.class);
    ColumnLineageConfig config = new ColumnLineageConfig();
    config.setTypedBoundaryFanInMaxEdges(3); // 2x2=4 > 3 => nothing
    when(olConfig.getColumnLineageConfig()).thenReturn(config);
    when(olContext.getOpenLineageConfig()).thenReturn(olConfig);
    when(builder.getContext()).thenReturn(olContext);

    Expression functionExpr = mockFunctionExpr(IN_1, IN_2);
    MapInPandas node = mock(MapInPandas.class);
    when(node.functionExpr()).thenReturn(functionExpr);
    when(node.output()).thenReturn(asSeq(OUT_1, OUT_2));

    visitor.apply(node, builder);

    verify(builder, never()).addDependency(any(), any(), any());
    verify(builder, never()).addDatasetDependency(any());
  }

  // ---- apply: edge cases ----

  @Test
  void emptyOutputEmitsNothing() {
    Expression functionExpr = mockFunctionExpr(IN_1, IN_2);
    MapInPandas node = mock(MapInPandas.class);
    when(node.functionExpr()).thenReturn(functionExpr);
    when(node.output()).thenReturn(asSeq());

    visitor.apply(node, builder);

    verify(builder, never()).addDependency(any(), any(), any());
  }

  @Test
  void nullFunctionExprEmitsNothing() {
    MapInPandas node = mock(MapInPandas.class);
    when(node.functionExpr()).thenReturn(null);
    when(node.output()).thenReturn(asSeq(OUT_1, OUT_2));

    visitor.apply(node, builder);

    verify(builder, never()).addDependency(any(), any(), any());
  }

  @Test
  void selfEdgesAreSkipped() {
    // output and input share an exprId => self-edge skipped
    Expression functionExpr = mockFunctionExpr(IN_1, IN_2);
    MapInPandas node = mock(MapInPandas.class);
    when(node.functionExpr()).thenReturn(functionExpr);
    // output includes IN_1 (EXPR_ID_1) which is also an input
    when(node.output()).thenReturn(asSeq(IN_1, OUT_1));

    visitor.apply(node, builder);

    TransformationInfo expected = indirectTransformation("MapInPandas");
    // IN_1<-IN_1 is a self-edge, skipped. Total edges: 2 outputs x 2 inputs - 1 self = 3
    verify(builder, never()).addDependency(eq(EXPR_ID_1), eq(EXPR_ID_1), eq(expected));
    verify(builder, times(1)).addDependency(eq(EXPR_ID_1), eq(EXPR_ID_2), eq(expected));
    verify(builder, times(1)).addDependency(eq(EXPR_ID_3), eq(EXPR_ID_1), eq(expected));
    verify(builder, times(1)).addDependency(eq(EXPR_ID_3), eq(EXPR_ID_2), eq(expected));
  }

  // ---- helpers ----

  /**
   * Creates a mock {@link Expression} whose {@code references()} returns an {@link AttributeSet}
   * built from the given attributes — the same set {@code PythonUDF.references()} would return for
   * those children.
   */
  private static Expression mockFunctionExpr(Attribute... inputs) {
    Expression mock = mock(Expression.class);
    AttributeSet attrSet = AttributeSet.apply(asSeq(inputs));
    when(mock.references()).thenReturn(attrSet);
    return mock;
  }

  private static TransformationInfo indirectTransformation(String description) {
    return new TransformationInfo(
        TransformationInfo.Types.INDIRECT,
        TransformationInfo.Subtypes.TRANSFORMATION,
        description,
        false);
  }
}
