/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.package$;
import org.junit.jupiter.api.Test;

/**
 * Cross-version compatibility guard for UDF and typed-operation column lineage.
 *
 * <p>Background: the {@code core} module (which is where all of the {@code
 * io.openlineage.spark3...plan.column} sources are compiled) is only present in the reactor for the
 * {@code Spark4} and {@code Spark41} profiles. The {@code Spark350} / {@code 14.3.dbr} / {@code
 * 15.4.dbr} / {@code 16.4.dbr} profiles build {@code api} alone, so this code is neither compiled
 * nor shipped there. Consequently this test can only ever execute on Spark 4.0 / 4.1.
 *
 * <p>Purpose: the plan nodes and expressions that UDF and typed-map lineage work
 * hangs off are all present with identical signatures across Spark 3.5.0, 4.0.0 and 4.1.2. That
 * makes conditional compilation unnecessary. This test locks that invariant in so that a future
 * Spark/DBR bump which removes or relocates one of these classes fails loudly here rather than
 * silently degrading lineage at runtime (the visitor dispatch in {@link VisitorFactory} would simply
 * stop matching, emitting no edges and no warning).
 *
 * <p>If a class listed below genuinely disappears in a newer runtime, do NOT delete it from this
 * list. Move it to the reflective soft-guard pattern already used by {@link ExpressionTraverser}
 * (its {@code classNames} list, matched on canonical name) so the code degrades gracefully instead
 * of failing to link.
 */
class TypedOperationCompatibilityTest {

  /**
   * Typed dataset operator plan nodes that typed-map lineage dispatches on. Verified
   * present in spark-catalyst 3.5.0 (2.12 and 2.13), 4.0.0 (2.13) and 4.1.2 (2.13).
   */
  private static final List<String> TYPED_OPERATOR_NODES =
      Arrays.asList(
          "org.apache.spark.sql.catalyst.plans.logical.MapElements",
          "org.apache.spark.sql.catalyst.plans.logical.MapPartitions",
          "org.apache.spark.sql.catalyst.plans.logical.TypedFilter",
          "org.apache.spark.sql.catalyst.plans.logical.SerializeFromObject",
          "org.apache.spark.sql.catalyst.plans.logical.DeserializeToObject",
          "org.apache.spark.sql.catalyst.plans.logical.AppendColumns",
          "org.apache.spark.sql.catalyst.plans.logical.AppendColumnsWithObject",
          "org.apache.spark.sql.catalyst.plans.logical.MapGroups",
          "org.apache.spark.sql.catalyst.plans.logical.CoGroup");

  /** UDF expressions that UDF lineage classification dispatches on. */
  private static final List<String> UDF_EXPRESSIONS =
      Arrays.asList(
          "org.apache.spark.sql.catalyst.expressions.ScalaUDF",
          "org.apache.spark.sql.catalyst.expressions.PythonUDF");

  private static boolean isLoadable(String className) {
    try {
      Class.forName(
          className, false, TypedOperationCompatibilityTest.class.getClassLoader());
      return true;
    } catch (ClassNotFoundException | LinkageError e) {
      return false;
    }
  }

  private static List<String> missing(List<String> classNames) {
    return classNames.stream().filter(c -> !isLoadable(c)).collect(Collectors.toList());
  }

  @Test
  void sanityCheckRunsOnSpark4OrLater() {
    // core is only in the reactor for Spark4/Spark41, so guard the assumption explicitly:
    // if this ever fails, the module layout changed and the reasoning in this test's javadoc
    // (and in docs/design/udf-column-lineage.md) needs revisiting.
    assertThat(package$.MODULE$.SPARK_VERSION())
        .as("core module is only built for the Spark4/Spark41 profiles")
        .startsWith("4");
  }

  @Test
  void allTypedOperatorPlanNodesArePresent() {
    assertThat(missing(TYPED_OPERATOR_NODES))
        .as(
            "typed dataset plan nodes must be linkable for typed-map lineage to dispatch; "
                + "move any genuinely absent class to ExpressionTraverser's reflective "
                + "classNames soft-guard instead of dropping it")
        .isEmpty();
  }

  @Test
  void allUdfExpressionsArePresent() {
    assertThat(missing(UDF_EXPRESSIONS))
        .as("UDF expressions must be linkable for UDF lineage classification to dispatch")
        .isEmpty();
  }

  /**
   * {@link ExpressionTraverser} keeps {@code Mask} in a reflective, canonical-name-matched list
   * because it was absent from older Spark versions. Across the range this project currently
   * supports (3.5.0, 4.0.0, 4.1.2) it is in fact always present. The soft guard is retained
   * deliberately: it costs nothing and it is the pattern to reuse for genuinely version-variant
   * classes, notably on Databricks runtimes whose jars are not resolvable in CI.
   */
  @Test
  void maskIsPresentButIntentionallySoftGuarded() {
    assertThat(isLoadable("org.apache.spark.sql.catalyst.expressions.Mask"))
        .as("Mask is present on all currently supported runtimes")
        .isTrue();
  }
}
