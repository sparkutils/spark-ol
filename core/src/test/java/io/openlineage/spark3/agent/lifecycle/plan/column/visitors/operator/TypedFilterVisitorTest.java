/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

import static io.openlineage.client.utils.TransformationInfo.Subtypes.FILTER;
import static io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.TypedOperationFixtures.EXPR_ID_1;
import static io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.TypedOperationFixtures.EXPR_ID_2;
import static io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.TypedOperationFixtures.NAME_1;
import static io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.TypedOperationFixtures.childWithOutput;
import static io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.TypedOperationFixtures.emptySchema;
import static io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.TypedOperationFixtures.field;
import static io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator.TypedOperationFixtures.twoFieldExpression;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.TypedFilter;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** Unit tests for {@link TypedFilterVisitor}, driven directly rather than through the registry. */
class TypedFilterVisitorTest {

  private final ColumnLevelLineageBuilder builder = mock(ColumnLevelLineageBuilder.class);
  private final TypedFilterVisitor visitor = new TypedFilterVisitor();

  @Test
  void isDefinedAtOnlyForTypedFilter() {
    assertTrue(visitor.isDefinedAt(typedFilter()));
    assertFalse(visitor.isDefinedAt(mock(LogicalPlan.class)));
  }

  /**
   * Typed filters pass their child's output through untouched, so no output column may be claimed as
   * derived from the predicate. Only a dataset-level INDIRECT/FILTER dependency is recorded, matching
   * how the untyped {@link FilterVisitor} reports a WHERE clause.
   */
  @Test
  void appliesDatasetLevelFilterDependency() {
    visitor.apply(typedFilter(), builder);

    ArgumentCaptor<ExprId> datasetDependency = ArgumentCaptor.forClass(ExprId.class);
    verify(builder, times(1)).addDatasetDependency(datasetDependency.capture());
    ExprId synthetic = datasetDependency.getValue();

    verify(builder, times(1))
        .addDependency(synthetic, EXPR_ID_1, TransformationInfo.indirect(FILTER));
    verify(builder, times(1))
        .addDependency(synthetic, EXPR_ID_2, TransformationInfo.indirect(FILTER));
    verifyNoMoreInteractions(builder);
  }

  private TypedFilter typedFilter() {
    return new TypedFilter(
        new Object(),
        Object.class,
        emptySchema(),
        twoFieldExpression(),
        childWithOutput(field(NAME_1, EXPR_ID_1)));
  }
}
