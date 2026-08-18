/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The single place that pins the column-lineage {@code VisitorFactory} registry. If you add a
 * visitor, add it here rather than asserting the registry from another suite.
 *
 * <p>The three typed visitors are disjoint by plan node, which is what lets them coexist without
 * double-claiming one:
 *
 * <ul>
 *   <li>{@code TypedFilterVisitor} on {@code TypedFilter}, unflagged — the deserializer names real
 *       attributes, so {@code INDIRECT/FILTER} is provable (over-broad, never a false pairing).
 *   <li>{@code TypedGroupByVisitor} on {@code MapGroups}, unflagged, same reasoning for {@code
 *       INDIRECT/GROUP_BY}.
 *   <li>{@code TypedBoundaryFanInVisitor} on {@code SerializeFromObject} only, gated behind {@code
 *       typedBoundaryFanInEnabled} which ships {@code false}. It is scoped at the boundary rather
 *       than per typed node because the defect is precisely output-attr → leaf-attr across
 *       Serialize/Deserialize. See {@code docs/design/typed-boundary-emission-policy.md}.
 * </ul>
 */
class ColumnLineageVisitorRegistryTest {

  private static List<String> registeredOperatorVisitors() {
    return new VisitorFactory()
        .operatorVisitors().stream()
            .map(visitor -> visitor.getClass().getSimpleName())
            .collect(Collectors.toList());
  }

  private static List<String> registeredExpressionVisitors() {
    return new VisitorFactory()
        .expressionVisitors().stream()
            .map(visitor -> visitor.getClass().getSimpleName())
            .collect(Collectors.toList());
  }

  @Test
  @DisplayName("the complete operator-visitor registry")
  void operatorVisitorRegistryIsExactlyThis() {
    assertThat(registeredOperatorVisitors())
        .containsExactlyInAnyOrder(
            "ProjectVisitor",
            "GenerateVisitor",
            "CreateTableAsSelectVisitor",
            "DistinctVisitor",
            "AggregateVisitor",
            "JoinVisitor",
            "FilterVisitor",
            "TypedFilterVisitor",
            "SortVisitor",
            "WindowVisitor",
            "DataSourceV2RelationVisitor",
            "UnionVisitor",
            "IcebergMergeIntoVisitor",
            "TypedBoundaryFanInVisitor",
            "TypedGroupByVisitor");
  }

  @Test
  @DisplayName("the fan-in stays boundary-scoped: no per-typed-node visitor is registered")
  void noPerTypedNodeVisitorIsRegistered() {
    // A visitor per MapElements / MapPartitions / AppendColumns / DeserializeToObject /
    // SerializeFromObject would claim the same boundary the fan-in already claims, emit even with
    // typedBoundaryFanInEnabled=false, and produce DIRECT edges across an opaque closure. TypedFilter
    // and MapGroups are the deliberate exceptions and are asserted positively above.
    assertThat(registeredOperatorVisitors())
        .noneMatch(
            name ->
                name.startsWith("MapElements")
                    || name.startsWith("MapPartitions")
                    || name.startsWith("AppendColumns")
                    || name.startsWith("DeserializeToObject")
                    || name.startsWith("SerializeFromObject"));
  }

  @Test
  @DisplayName("the expression registry carries the UDF opacity visitor")
  void expressionVisitorRegistryIsExactlyThis() {
    // UserDefinedExpressionVisitor only has to precede ExpressionTraverser's generic children()
    // fallback, which being registered at all achieves.
    assertThat(registeredExpressionVisitors())
        .containsExactlyInAnyOrder(
            "AliasVisitor",
            "CaseWhenVisitor",
            "IfVisitor",
            "CoalesceVisitor",
            "AggregateExpressionVisitor",
            "WindowVisitor",
            "UserDefinedExpressionVisitor");
  }
}
