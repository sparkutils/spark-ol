/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.util.Arrays;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.EqualTo;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata$;
import org.apache.spark.sql.types.StructType;
import scala.collection.immutable.Seq;

/**
 * Fixtures for the typed (Dataset API) operator visitor tests.
 *
 * <p>Deliberately self contained: the upstream {@code ColumnLevelFixtures} lives in the OpenLineage
 * submodule's own test tree, which is not part of this module's test sources.
 */
public final class TypedOperationFixtures {

  public static final String NAME_1 = "name1";
  public static final String NAME_2 = "name2";

  public static final ExprId EXPR_ID_1 = ExprId.apply(21);
  public static final ExprId EXPR_ID_2 = ExprId.apply(22);

  private TypedOperationFixtures() {}

  public static AttributeReference field(String name, ExprId exprId) {
    return new AttributeReference(
        name,
        IntegerType$.MODULE$,
        false,
        Metadata$.MODULE$.empty(),
        exprId,
        ScalaConversionUtils.asScalaSeqEmpty());
  }

  /**
   * Stands in for the encoder deserializer/serializer bodies: a non-leaf expression reading two
   * attributes, so the traverser has to descend into children the way it does for a real {@code
   * NewInstance} / {@code CreateExternalRow} tree.
   */
  public static EqualTo twoFieldExpression() {
    return new EqualTo(field(NAME_1, EXPR_ID_1), field(NAME_2, EXPR_ID_2));
  }

  public static LogicalPlan childWithOutput(AttributeReference... attributes) {
    LogicalPlan child = mock(LogicalPlan.class);
    when(child.output()).thenReturn(asSeq(attributes));
    return child;
  }

  public static StructType emptySchema() {
    return new StructType();
  }

  @SafeVarargs
  public static <T> Seq<T> asSeq(T... elements) {
    return ScalaConversionUtils.fromList(Arrays.asList(elements));
  }
}
