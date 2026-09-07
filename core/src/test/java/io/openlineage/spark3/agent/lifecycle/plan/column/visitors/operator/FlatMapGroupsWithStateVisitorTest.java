/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

import static io.openlineage.client.utils.TransformationInfo.Subtypes.GROUP_BY;
import static io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.TypedOperationFixtures.EXPR_ID_1;
import static io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.TypedOperationFixtures.NAME_1;
import static io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.TypedOperationFixtures.asSeq;
import static io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.TypedOperationFixtures.field;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.plans.logical.FlatMapGroupsWithState;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.MapGroups;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * {@link TypedGroupByVisitor} also matches {@link FlatMapGroupsWithState} — the node behind {@code
 * groupByKey().mapGroupsWithState/flatMapGroupsWithState} — so a stateful typed grouping records the
 * same {@code INDIRECT/GROUP_BY} signal as {@link MapGroups}.
 *
 * <p>Driven directly rather than through a real streaming plan: the streaming source needed to
 * produce a {@code FlatMapGroupsWithState} pulls in a {@code spark-sql} connector class absent from
 * this module's test classpath (the same Spark 4.0.0 gap {@code TypedLineageTestBase} documents).
 */
class FlatMapGroupsWithStateVisitorTest {

  private final TypedGroupByVisitor visitor = new TypedGroupByVisitor();
  private final ColumnLevelLineageBuilder builder = mock(ColumnLevelLineageBuilder.class);

  @Test
  void isDefinedAtForMapGroupsAndFlatMapGroupsWithState() {
    assertTrue(visitor.isDefinedAt(mock(MapGroups.class)));
    assertTrue(visitor.isDefinedAt(mock(FlatMapGroupsWithState.class)));
    assertFalse(visitor.isDefinedAt(mock(LogicalPlan.class)));
  }

  /**
   * The dispatch must read {@code groupingAttributes()} and {@code child()} from the {@code
   * FlatMapGroupsWithState} node, not cast it to {@code MapGroups}. A {@code ClassCastException}
   * here would mean the visitor's {@code isDefinedAt} and {@code apply} disagree.
   */
  @Test
  void appliesGroupByDependencyForFlatMapGroupsWithState() {
    FlatMapGroupsWithState node = mock(FlatMapGroupsWithState.class);
    // A grouping attribute with no AppendColumns above it is used as-is (a grouping on a real
    // column), which is the correct fallback and keeps this test free of AppendColumns plumbing.
    when(node.groupingAttributes()).thenReturn(asSeq(field(NAME_1, EXPR_ID_1)));
    LogicalPlan child = mock(LogicalPlan.class);
    when(child.children()).thenReturn(ScalaConversionUtils.asScalaSeqEmpty());
    when(node.child()).thenReturn(child);

    visitor.apply(node, builder);

    ArgumentCaptor<ExprId> datasetDependency = ArgumentCaptor.forClass(ExprId.class);
    verify(builder, times(1)).addDatasetDependency(datasetDependency.capture());
    verify(builder, times(1))
        .addDependency(
            datasetDependency.getValue(), EXPR_ID_1, TransformationInfo.indirect(GROUP_BY));
  }
}
