/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@code MapGroups} — the node behind {@code groupByKey().mapGroups} — and its {@code
 * INDIRECT/GROUP_BY} edges.
 *
 * <p>Unflagged, like {@code TypedFilter}: the signal is structurally provable from the plan, and
 * gating a faithful signal behind the flag that exists to contain an unfaithful one would leave typed
 * groupings invisible to everyone who has not opted into an unrelated pessimistic behaviour.
 *
 * <p>The measurement that shapes the implementation: {@code MapGroups.groupingAttributes()} does
 * <b>not</b> name a source column — it names the synthetic key column that {@code AppendColumns}
 * appended, whose serializer is rooted at the opaque intermediate object and has no {@code
 * references()}. {@code TypedGroupByVisitor} therefore matches each grouping attribute back to the
 * {@code AppendColumns} whose {@code newColumns} carry that <b>exprId</b> and traverses that node's
 * {@code deserializer()}. Matching on exprId rather than name or position is what makes the descent
 * safe when a nested typed grouping sits deeper in the plan.
 *
 * @see TypedLineageTestBase for the harness
 */
class MapGroupsColumnLineageTest extends TypedLineageTestBase {

  /** {@code groupByKey(row -> row.getInt(0) % 2).mapGroups((key, rows) -> key)} over {@link #source()}. */
  private Dataset<Integer> groupedByKeyReadingColumnA() {
    return source()
        .groupByKey((MapFunction<Row, Integer>) row -> row.getInt(0) % 2, Encoders.INT())
        .mapGroups((MapGroupsFunction<Integer, Row, Integer>) (key, rows) -> key, Encoders.INT());
  }

  @Test
  @DisplayName("groupByKey().mapGroups records INDIRECT/GROUP_BY, with or without the fan-in")
  void typedGroupByKeyMapGroups() {
    Dataset<Integer> ds = groupedByKeyReadingColumnA();

    // MapGroups, not just AppendColumns - the original ticket body omitted MapGroups entirely.
    assertThat(planChain(ds)).isEqualTo("SerializeFromObject>MapGroups>AppendColumns>LogicalRDD");

    // The typed grouping IS recorded, unflagged, because it is a faithful signal rather than a
    // pessimistic one. See TypedGroupByVisitor's javadoc for the breadth caveat.
    //
    // Collected ONCE: a dataset dependency mints a fresh ExprId on every collection pass, so
    // calling edges() twice yields two different ids for the same behaviour. Asserting on the id at
    // all is deliberate - it pins that all three inputs hang off ONE dataset dependency rather than
    // three unrelated ones, which is what makes it a single grouping.
    List<String> groupByEdges = edges(ds);
    long dependencyId = soleDatasetDependencyId(groupByEdges, "INDIRECT/GROUP_BY");
    assertThat(groupByEdges)
        .containsExactlyInAnyOrder(
            "#" + dependencyId + "<-a INDIRECT/GROUP_BY/mask=false",
            "#" + dependencyId + "<-b INDIRECT/GROUP_BY/mask=false",
            "#" + dependencyId + "<-s INDIRECT/GROUP_BY/mask=false");

    // Control: the untyped equivalent records the same subtype.
    assertThat(edges(source().groupBy("a").count()))
        .anyMatch(edge -> edge.contains("INDIRECT/GROUP_BY"));

    // Facet level with the fan-in off: still empty. The GROUP_BY edge is a DATASET dependency, and
    // with datasetLineageEnabled=false a dataset dependency is folded into fields that already have
    // inputs - and no output field of the boundary has any, since nothing links SerializeFromObject's
    // output back to the leaf without the fan-in.
    assertThat(facetFields(ds)).isEmpty();

    // With the fan-in on the boundary supplies that missing link, and the GROUP_BY dataset
    // dependency then rides along on the field. Both transformations are present per input field:
    // the fan-in's TRANSFORMATION and this visitor's GROUP_BY.
    assertThat(facetFieldsWithTypedBoundaryFanIn(ds))
        .containsExactlyInAnyOrder(
            "value<-a INDIRECT/TRANSFORMATION/mask=false",
            "value<-b INDIRECT/TRANSFORMATION/mask=false",
            "value<-s INDIRECT/TRANSFORMATION/mask=false",
            "value<-a INDIRECT/GROUP_BY/mask=false",
            "value<-b INDIRECT/GROUP_BY/mask=false",
            "value<-s INDIRECT/GROUP_BY/mask=false");
  }

  @Test
  @DisplayName("the typed GROUP_BY edge is independent of the fan-in flag")
  void typedGroupByIsNotGatedBehindTheFanInFlag() {
    Dataset<Integer> ds = groupedByKeyReadingColumnA();

    // Exactly three GROUP_BY edges either way. The fan-in adds TRANSFORMATION edges on top, it
    // neither creates nor suppresses the grouping signal.
    assertThat(edges(ds)).filteredOn(edge -> edge.contains("GROUP_BY")).hasSize(3);
    assertThat(edgesWithTypedBoundaryFanIn(ds, null))
        .filteredOn(edge -> edge.contains("GROUP_BY"))
        .hasSize(3);

    // And it survives the fan-in's width cap, which is a no-emit for the fan-in only.
    assertThat(edgesWithTypedBoundaryFanIn(ds, 0))
        .filteredOn(edge -> edge.contains("GROUP_BY"))
        .hasSize(3);
    assertThat(edgesWithTypedBoundaryFanIn(ds, 0))
        .noneMatch(edge -> edge.contains("TRANSFORMATION"));
  }

  @Test
  @DisplayName("the GROUP_BY breadth is the deserialized schema, not the key lambda's reads")
  void typedGroupByOverClaimsBreadth() {
    // The key function reads column "a" only. The edge set is nevertheless all three columns,
    // because AppendColumns.deserializer materialises the whole Row before the lambda runs and the
    // lambda body is bytecode. This is the same over-breadth TypedFilterVisitor documents; it is
    // over-broad but never a false specific pairing. Pinned so that narrowing it (which needs
    // bytecode analysis) shows up as a diff here.
    assertThat(edges(groupedByKeyReadingColumnA()))
        .filteredOn(edge -> edge.contains("GROUP_BY"))
        .hasSize(3)
        .anyMatch(edge -> edge.endsWith("<-b INDIRECT/GROUP_BY/mask=false"))
        .anyMatch(edge -> edge.endsWith("<-s INDIRECT/GROUP_BY/mask=false"));
  }
}
