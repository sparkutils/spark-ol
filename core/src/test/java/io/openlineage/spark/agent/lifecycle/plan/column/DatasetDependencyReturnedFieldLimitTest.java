/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan.column;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Boundary tests for {@code RETURNED_INPUT_FIELD_LIMIT} in the presence of <em>dataset-level</em>
 * dependencies (those registered by {@code addDatasetDependency}, i.e. what {@code FilterVisitor},
 * {@code SortVisitor}, {@code JoinVisitor}, {@code AggregateVisitor} and {@code TypedFilterVisitor}
 * emit).
 *
 * <p>Before this fix the limit was tested against the per-field dependency lists only, while dataset
 * -level dependencies were merged into every field's input list <em>afterwards</em>, inside {@code
 * facetInputFields}. They therefore escaped the guard entirely and were returned however many there
 * were — measured at 122 850 rendered entries against a 100 000 limit (see {@code
 * docs/design/typed-boundary-emission-policy.md}).
 *
 * <p>The rendered cost of one dataset-level dependency is not 1 but {@code (emitted output fields)},
 * because it is replicated onto each of them. These tests pin that arithmetic at the boundary, and
 * are written directly against the builder because the accounting lives there and is shared by every
 * dataset-dependency emitter — this is the untyped {@code Filter}/{@code Join}/{@code Aggregate} path
 * as much as the typed one.
 */
class DatasetDependencyReturnedFieldLimitTest {

  /**
   * The production constant, mirrored. Deliberately duplicated rather than made visible: if someone
   * changes the limit, these boundary widths are wrong and MUST be recomputed, and a compile-time
   * link would hide that.
   */
  private static final int RETURNED_INPUT_FIELD_LIMIT = 100_000;

  private static final DatasetIdentifier SOURCE = new DatasetIdentifier("src", "ns");

  private final OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
  private final OpenLineageContext context = mock(OpenLineageContext.class);

  @BeforeEach
  void setup() {
    SparkOpenLineageConfig config = new SparkOpenLineageConfig();
    config.getColumnLineageConfig().setDatasetLineageEnabled(false);
    when(context.getOpenLineage()).thenReturn(openLineage);
    when(context.getOpenLineageConfig()).thenReturn(config);
  }

  /**
   * The shape a dataset-dependency emitter produces over a {@code width}-column pass-through
   * relation: every column is an output field carrying its own identity input (1 per-field
   * dependency each), and every column is additionally registered as a dataset-level dependency.
   *
   * <p>Rendered facet size is therefore {@code width} (identities) + {@code width * width} (each of
   * the width dataset dependencies replicated onto each of the width fields) = {@code width * (width
   * + 1)} transformation entries, which is exactly the growth measured for a typed filter. Note the
   * emitted {@code InputField} count is only {@code width * width}, because each field's own
   * identity groups with the dataset-level dependency naming the same input column.
   */
  private ColumnLevelLineageBuilder passThroughWithDatasetDependencyPerColumn(int width) {
    OpenLineage.SchemaDatasetFacet schema =
        openLineage.newSchemaDatasetFacet(
            IntStream.range(0, width)
                .mapToObj(
                    i ->
                        openLineage
                            .newSchemaDatasetFacetFieldsBuilder()
                            .name("c" + i)
                            .type("int")
                            .build())
                .collect(Collectors.toList()));

    ColumnLevelLineageBuilder builder = new ColumnLevelLineageBuilder(schema, context);
    for (int i = 0; i < width; i++) {
      ExprId exprId = ExprId.apply(i + 1L);
      builder.addOutput(exprId, "c" + i);
      builder.addInput(exprId, SOURCE, "c" + i);
      builder.addDatasetDependency(exprId);
    }
    return builder;
  }

  /**
   * The unit the limit is expressed in: {@code TransformedInput} entries.
   *
   * <p>This is what the pre-existing check counted — {@code fieldsDependencies} sums the sizes of
   * the per-field {@code List<TransformedInput>} — so the dataset-level contribution must be
   * counted in the same unit to be summable with it. One entry becomes one {@code transformations[]}
   * element in the emitted facet.
   */
  private static int renderedTransformations(OpenLineage.ColumnLineageDatasetFacetFields fields) {
    return fields.getAdditionalProperties().values().stream()
        .flatMap(f -> f.getInputFields().stream())
        .map(input -> input.getTransformations().size())
        .reduce(0, Integer::sum);
  }

  /**
   * The count of emitted {@code InputField} objects, which is strictly smaller than {@link
   * #renderedTransformations} because {@code facetInputFields} groups by {@code Input}: a per-field
   * identity on {@code c_i} and the dataset-level dependency on {@code c_i} collapse into one {@code
   * InputField} carrying two transformations.
   *
   * <p>Asserted alongside the transformation count so the two units are never silently conflated —
   * the guard is expressed in the former, and reading it as the latter is what made the boundary
   * widths below look wrong on first run.
   */
  private static int renderedInputFields(OpenLineage.ColumnLineageDatasetFacetFields fields) {
    return fields.getAdditionalProperties().values().stream()
        .map(f -> f.getInputFields().size())
        .reduce(0, Integer::sum);
  }

  // ---------------------------------------------------------------------------
  // the boundary: width * (width + 1) either side of 100_000
  // ---------------------------------------------------------------------------

  @Test
  @DisplayName("just UNDER the limit: dataset dependencies are returned in full")
  void datasetDependenciesJustUnderTheLimitAreReturned() {
    int width = 315; // 315 * 316 = 99_540
    assertThat(width * (width + 1)).isLessThanOrEqualTo(RETURNED_INPUT_FIELD_LIMIT);

    OpenLineage.ColumnLineageDatasetFacetFields fields =
        passThroughWithDatasetDependencyPerColumn(width).buildFields(false);

    assertThat(fields.getAdditionalProperties()).hasSize(width);
    assertThat(renderedTransformations(fields)).isEqualTo(width * (width + 1));
    assertThat(renderedInputFields(fields)).isEqualTo(width * width);
  }

  @Test
  @DisplayName("just OVER the limit: the facet collapses to empty (previously returned in full)")
  void datasetDependenciesJustOverTheLimitCollapseToEmpty() {
    int width = 316; // 316 * 317 = 100_172
    assertThat(width * (width + 1)).isGreaterThan(RETURNED_INPUT_FIELD_LIMIT);

    OpenLineage.ColumnLineageDatasetFacetFields fields =
        passThroughWithDatasetDependencyPerColumn(width).buildFields(false);

    // BEHAVIOUR CHANGE. Before the accounting fix this returned 316 fields / 100_172 transformation
    // entries, because `fieldsDependencies` only saw the 316 per-field identities. It now trips the
    // guard, which is what the limit exists to do.
    assertThat(fields.getAdditionalProperties()).isEmpty();
  }

  @Test
  @DisplayName("the fix is the replication factor, not the dependency count")
  void datasetDependencyCostIsPerOutputFieldNotPerDependency() {
    // 400 dataset dependencies is nowhere near 100_000 as a raw count — a naive
    // `+= datasetDependencies.size()` would let this through. Rendered, it is 400 * 401 = 160_400.
    int width = 400;
    assertThat(width).isLessThan(RETURNED_INPUT_FIELD_LIMIT);
    assertThat(width * (width + 1)).isGreaterThan(RETURNED_INPUT_FIELD_LIMIT);

    assertThat(
            passThroughWithDatasetDependencyPerColumn(width)
                .buildFields(false)
                .getAdditionalProperties())
        .isEmpty();
  }

  // ---------------------------------------------------------------------------
  // the accounting is a sum, so neither contribution alone tells the story
  // ---------------------------------------------------------------------------

  @Test
  @DisplayName("per-field and dataset-level contributions are summed, not checked independently")
  void perFieldAndDatasetLevelContributionsAreSummed() {
    // 200 fields, 200 dataset dependencies -> 200 * 201 = 40_200 rendered, comfortably under.
    int width = 200;
    OpenLineage.SchemaDatasetFacet schema =
        openLineage.newSchemaDatasetFacet(
            IntStream.range(0, width)
                .mapToObj(
                    i ->
                        openLineage
                            .newSchemaDatasetFacetFieldsBuilder()
                            .name("c" + i)
                            .type("int")
                            .build())
                .collect(Collectors.toList()));

    ColumnLevelLineageBuilder builder = new ColumnLevelLineageBuilder(schema, context);
    List<ExprId> outputs =
        IntStream.range(0, width).mapToObj(i -> ExprId.apply(i + 1L)).collect(Collectors.toList());
    for (int i = 0; i < width; i++) {
      builder.addOutput(outputs.get(i), "c" + i);
      builder.addInput(outputs.get(i), SOURCE, "c" + i);
      builder.addDatasetDependency(outputs.get(i));
    }
    assertThat(renderedTransformations(builder.buildFields(false))).isEqualTo(width * (width + 1));

    // Now the case the sum exists for: BOTH sides individually under the limit, together over.
    //
    // Note the dataset-level side must be built from exprIds that are NOT the outputs. Registering
    // an output as its own dataset dependency makes the two sides move together, because
    // datasetDependencyInputs() resolves each registered exprId TRANSITIVELY through
    // getInputsUsedFor — so 200 outputs carrying 321 inputs each resolve to 64 200 dataset-level
    // inputs, not 200. (Measured: an earlier version of this test asserted empty for that reason
    // while claiming a 40 000 dataset-level contribution. It passed for the wrong reason.)
    int extrasPerField = 300;
    int datasetDeps = 300;

    // per-field side alone: 200 * 300 = 60 000, under the limit and returned in full.
    ColumnLevelLineageBuilder perFieldOnly = new ColumnLevelLineageBuilder(schema, context);
    addPerFieldInputs(perFieldOnly, width, extrasPerField);
    assertThat(renderedTransformations(perFieldOnly.buildFields(false)))
        .isEqualTo(width * extrasPerField)
        .isLessThan(RETURNED_INPUT_FIELD_LIMIT);

    // dataset-level side alone: 300 dependencies replicated over 200 emitted fields = 60 000, plus
    // the 200 identities that make those fields emit at all. Under the limit and returned in full.
    ColumnLevelLineageBuilder datasetOnly = new ColumnLevelLineageBuilder(schema, context);
    for (int i = 0; i < width; i++) {
      ExprId out = ExprId.apply(i + 1L);
      datasetOnly.addOutput(out, "c" + i);
      datasetOnly.addInput(out, SOURCE, "c" + i);
    }
    addDatasetDependencies(datasetOnly, datasetDeps);
    assertThat(renderedTransformations(datasetOnly.buildFields(false)))
        .isEqualTo(width + datasetDeps * width)
        .isLessThan(RETURNED_INPUT_FIELD_LIMIT);

    // Together: 60 000 + 60 000 = 120 000 > 100 000. Neither check alone would have caught this.
    ColumnLevelLineageBuilder both = new ColumnLevelLineageBuilder(schema, context);
    addPerFieldInputs(both, width, extrasPerField);
    addDatasetDependencies(both, datasetDeps);
    assertThat(width * extrasPerField + datasetDeps * width)
        .isGreaterThan(RETURNED_INPUT_FIELD_LIMIT);
    assertThat(both.buildFields(false).getAdditionalProperties()).isEmpty();
  }

  /** Gives each of {@code width} outputs {@code extras} distinct inputs of its own. */
  private void addPerFieldInputs(ColumnLevelLineageBuilder builder, int width, int extras) {
    for (int i = 0; i < width; i++) {
      ExprId out = ExprId.apply(i + 1L);
      builder.addOutput(out, "c" + i);
      for (int j = 0; j < extras; j++) {
        ExprId extra = ExprId.apply(1_000_000L + i * 1_000L + j);
        builder.addInput(extra, SOURCE, "extra_" + i + "_" + j);
        builder.addDependency(out, extra);
      }
    }
  }

  /**
   * Registers {@code count} dataset-level dependencies on exprIds that are not outputs and carry one
   * input each, so the resolved dataset-level input count is exactly {@code count}.
   */
  private void addDatasetDependencies(ColumnLevelLineageBuilder builder, int count) {
    for (int d = 0; d < count; d++) {
      ExprId dep = ExprId.apply(9_000_000L + d);
      builder.addInput(dep, SOURCE, "dep_" + d);
      builder.addDatasetDependency(dep);
    }
  }

  @Test
  @DisplayName(
      "datasetLineageEnabled=true keeps dataset dependencies out of fields, so they cost nothing there")
  void datasetLineageEnabledExcludesDatasetDependenciesFromTheFieldAccounting() {
    // With the flag on, buildFields deliberately renders no dataset dependencies (they go to the
    // facet's `dataset` section instead via buildDatasetDependencies), so they must not be charged
    // against the fields limit either. Same width that collapses with the flag off.
    ColumnLevelLineageBuilder builder = passThroughWithDatasetDependencyPerColumn(400);

    OpenLineage.ColumnLineageDatasetFacetFields fields = builder.buildFields(true);

    assertThat(fields.getAdditionalProperties()).hasSize(400);
    assertThat(renderedTransformations(fields)).isEqualTo(400); // identities only
    assertThat(builder.buildDatasetDependencies(true)).isPresent();
  }

  @Test
  @DisplayName("a facet with no dataset dependencies is unaffected by the change")
  void perFieldOnlyAccountingIsUnchanged() {
    int width = 400;
    OpenLineage.SchemaDatasetFacet schema =
        openLineage.newSchemaDatasetFacet(
            IntStream.range(0, width)
                .mapToObj(
                    i ->
                        openLineage
                            .newSchemaDatasetFacetFieldsBuilder()
                            .name("c" + i)
                            .type("int")
                            .build())
                .collect(Collectors.toList()));

    ColumnLevelLineageBuilder builder = new ColumnLevelLineageBuilder(schema, context);
    for (int i = 0; i < width; i++) {
      ExprId exprId = ExprId.apply(i + 1L);
      builder.addOutput(exprId, "c" + i);
      builder.addInput(exprId, SOURCE, "c" + i);
    }

    // 400 rendered entries, no dataset dependencies: identical before and after the fix.
    OpenLineage.ColumnLineageDatasetFacetFields fields = builder.buildFields(false);
    assertThat(fields.getAdditionalProperties()).hasSize(width);
    assertThat(renderedTransformations(fields)).isEqualTo(width);
  }
}
