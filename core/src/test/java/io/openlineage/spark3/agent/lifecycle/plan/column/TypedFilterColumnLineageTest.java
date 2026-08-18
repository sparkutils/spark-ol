/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@code TypedFilter} — the node behind {@code Dataset.filter(T => Boolean)} and {@code
 * filter(FilterFunction)} — and its {@code INDIRECT/FILTER} edges.
 *
 * <p>Unflagged, because the signal is structurally provable from the plan and therefore independent
 * of the fan-in choice that {@link TypedBoundaryColumnLineageTest} covers. The registry assertion
 * lives in {@link ColumnLineageVisitorRegistryTest}, not here.
 *
 * <p>{@link #typedFilterIsUnaffectedByTheTypedBoundaryFanIn()} asserts both flag states, because
 * "this node is untouched by the fan-in" is a claim about two configurations and cannot be made from
 * one.
 *
 * @see TypedLineageTestBase for the harness
 */
class TypedFilterColumnLineageTest extends TypedLineageTestBase {

  @Test
  @DisplayName(
      "FIXED: typed filter(T => Boolean) now contributes INDIRECT/FILTER edges (over-broad — see below)")
  void typedFilterEmitsIndirectFilterEdgesOverTheWholeDeserializedSchema() {
    Dataset<Row> typed = source().filter((FilterFunction<Row>) row -> row.getInt(0) > 1);

    assertThat(planChain(typed)).isEqualTo("TypedFilter>LogicalRDD");

    // Exact edge set. The output id is a synthetic dataset-dependency exprId (rendered #<n>), the
    // same shape FilterVisitor uses, so it is normalised away here and asserted separately.
    assertThat(edgesWithoutOutputId(typed))
        .containsExactlyInAnyOrder(
            "<-a INDIRECT/FILTER/mask=false",
            "<-b INDIRECT/FILTER/mask=false",
            "<-s INDIRECT/FILTER/mask=false");

    // All three inputs hang off ONE dataset dependency, which is what makes it a single filter
    // rather than three unrelated restrictions.
    assertThat(syntheticOutputIds(typed)).hasSize(1);

    // The identity fields are still inferred edge-free (output and child share exprIds), so the
    // new edges ADD the row-restriction signal rather than replacing the pass-through.
    assertThat(facetFields(typed))
        .contains(
            "a<-a DIRECT/IDENTITY/mask=false",
            "b<-b DIRECT/IDENTITY/mask=false",
            "s<-s DIRECT/IDENTITY/mask=false")
        .anyMatch(field -> field.contains("INDIRECT/FILTER"));
  }

  @Test
  @DisplayName(
      "LIMITATION: the typed filter edge set is the whole deserialized schema, not the predicate's columns")
  void typedFilterEdgesAreBroaderThanThePredicateActuallyReads() {
    // The predicate reads column `a` only. `b` and `s` are never touched by the lambda.
    Dataset<Row> typed = source().filter((FilterFunction<Row>) row -> row.getInt(0) > 1);

    // Yet all three columns are reported, because TypedFilter's deserializer materialises the
    // entire Row before the lambda runs; the field access itself lives in opaque lambda bytecode
    // and is not in the plan. This is over-breadth, NOT parity with the untyped path.
    assertThat(edgesWithoutOutputId(typed))
        .containsExactlyInAnyOrder(
            "<-a INDIRECT/FILTER/mask=false",
            "<-b INDIRECT/FILTER/mask=false",
            "<-s INDIRECT/FILTER/mask=false");

    // The untyped equivalent of the SAME predicate names only `a`. (It emits that edge twice —
    // the optimizer rewrites the condition to `isnotnull(a) AND (a > 1)`, so `a` is referenced
    // twice — hence containsOnly rather than containsExactly. The duplicate is pre-existing
    // untyped-path behaviour, unrelated to TypedFilter.)
    Dataset<Row> untyped = source().filter(functions.col("a").gt(1));
    assertThat(edgesWithoutOutputId(untyped)).containsOnly("<-a INDIRECT/FILTER/mask=false");

    // Stated as the invariant that matters: over-broad, never a false specific pairing — every
    // emitted edge names a column the deserializer genuinely read.
    assertThat(edgesWithoutOutputId(typed)).containsAll(edgesWithoutOutputId(untyped));
  }

  @Test
  @DisplayName("typed filter is NOT touched by the fan-in: it never crosses the boundary")
  void typedFilterIsUnaffectedByTheTypedBoundaryFanIn() {
    Dataset<Row> typed = source().filter((FilterFunction<Row>) row -> true);

    assertThat(planChain(typed)).isEqualTo("TypedFilter>LogicalRDD");

    // No SerializeFromObject, so TypedBoundaryFanInVisitor is never defined at this plan. TypedFilter
    // passes its child's attributes through unchanged, sharing their exprIds, so identity is already
    // inferred edge-free; replacing that faithful pass-through with an opaque fan-in would be a
    // regression. Asserted as EQUALITY of the two configurations rather than as two separate expected
    // sets, so the claim degrades correctly if either side changes.
    assertThat(facetFieldsWithTypedBoundaryFanIn(typed))
        .containsExactlyInAnyOrderElementsOf(facetFields(typed));

    // Both configurations carry the identity pass-through AND the filter signal.
    assertThat(facetFieldsWithTypedBoundaryFanIn(typed))
        .contains(
            "a<-a DIRECT/IDENTITY/mask=false",
            "b<-b DIRECT/IDENTITY/mask=false",
            "s<-s DIRECT/IDENTITY/mask=false")
        .anyMatch(field -> field.contains("INDIRECT/FILTER"));

    // No fan-in TRANSFORMATION edge is contributed either way - that is what "never crosses the
    // boundary" means, and it is the assertion that would fail if the fan-in were rescoped to match
    // typed nodes individually instead of the Serialize boundary.
    assertThat(facetFieldsWithTypedBoundaryFanIn(typed))
        .noneMatch(field -> field.contains("INDIRECT/TRANSFORMATION"));

    // Control: the untyped equivalent records the filter too, so this is parity in kind (though not
    // in breadth - see typedFilterEdgesAreBroaderThanThePredicateActuallyReads).
    assertThat(facetFields(source().filter(functions.col("a").gt(1))))
        .anyMatch(field -> field.contains("INDIRECT/FILTER"));
  }

  @Test
  @DisplayName(
      "typed filter breadth renders quadratically, and IS now counted against RETURNED_INPUT_FIELD_LIMIT")
  void typedFilterFacetGrowsQuadraticallyWithTableWidth() {
    // A typed filter contributes one dataset-level dependency per deserialized column (W), and
    // buildFields replicates every dataset dependency onto every output field (W) — so the facet
    // carries W*(W+1) entries, against the untyped path's W*2.
    Dataset<Row> wide = wideSource(12);

    List<String> typedFacet =
        facetFields(wide.filter((FilterFunction<Row>) row -> row.getInt(0) > 0));
    List<String> untypedFacet = facetFields(wide.filter(functions.col("c0").gt(0)));

    assertThat(typedFacet).hasSize(156); // 12 outputs * (12 filter + 1 identity)
    assertThat(untypedFacet).hasSize(24); // 12 outputs * (1 filter + 1 identity)

    // The quadratic growth itself is a property of facetInputFields replicating dataset-level
    // dependencies onto every output field, not of the limit accounting. What changed is that the
    // growth is now visible to RETURNED_INPUT_FIELD_LIMIT: buildFields adds (dataset dependencies x
    // emitted fields) to the per-field sum before testing the limit. At width 12 the rendered size is
    // 156, far under 100_000, so the facet is returned in full.
    assertThat(typedFacet).isNotEmpty();
  }

  @Test
  @DisplayName(
      "a wide typed filter collapses to empty instead of returning 122 850 entries")
  void wideTypedFilterCollapsesToEmptyOnceDatasetDependenciesAreCounted() {
    // The width that used to bypass the guard: 350 * 351 = 122 850 rendered entries against a
    // 100 000 limit. Before the accounting fix all 122 850 were returned, because fieldsDependencies
    // summed only the 350 per-field identities and the 350 dataset-level dependencies were merged in
    // afterwards inside facetInputFields. It now trips the guard.
    Dataset<Row> wide = wideSource(350);

    assertThat(facetFields(wide.filter((FilterFunction<Row>) row -> row.getInt(0) > 0))).isEmpty();

    // The untyped filter over the SAME 350-column table is unaffected: it contributes O(predicate
    // columns) = 1 dataset dependency, so 350 identities + 350 * 1 replicated = 700, well under.
    // This is the shape that keeps the change invisible to the untyped path in practice.
    assertThat(facetFields(wide.filter(functions.col("c0").gt(0)))).hasSize(700);
  }

  @Test
  @DisplayName(
      "the untyped path is reachable too, given enough predicate columns")
  void wideUntypedFilterAlsoCollapsesOnceDatasetDependenciesAreCounted() {
    // The change is not typed-path-specific. An untyped Filter contributes one dataset dependency per distinct column its predicate reads, so
    // a predicate spanning every column of a 350-wide table reaches the same 350 * 351 = 122 850
    // rendered size that a typed filter reaches by construction, and now collapses identically.
    int width = 350;
    Dataset<Row> wide = wideSource(width);

    // Built as a BALANCED or-tree, not a left fold. A 350-deep `a.or(b).or(c)...` chain overflows
    // the analyzer's recursive tree traversal with a StackOverflowError before lineage is ever
    // reached — measured, not hypothesised. Balancing brings the depth to ceil(log2(350)) = 9.
    List<org.apache.spark.sql.Column> terms = new ArrayList<>();
    for (int i = 0; i < width; i++) {
      terms.add(functions.col("c" + i).gt(0));
    }
    while (terms.size() > 1) {
      List<org.apache.spark.sql.Column> next = new ArrayList<>();
      for (int i = 0; i < terms.size(); i += 2) {
        next.add(i + 1 < terms.size() ? terms.get(i).or(terms.get(i + 1)) : terms.get(i));
      }
      terms = next;
    }

    // Sanity: this is a plain untyped Filter, no typed node in the plan at all — so no part of this
    // collapse can be attributed to TypedFilterVisitor.
    Dataset<Row> filtered = wide.filter(terms.get(0));
    assertThat(planChain(filtered)).isEqualTo("Filter>LogicalRDD");

    assertThat(facetFields(filtered)).isEmpty();
  }
}
