/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StatefulProcessor;
import org.apache.spark.sql.streaming.StatefulProcessorWithInitialState;
import org.apache.spark.sql.streaming.TimeMode;
import org.apache.spark.sql.streaming.TimerValues;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@code TransformWithState} — the node behind {@code groupByKey().transformWithState} (Spark 4+
 * arbitrary state API v2) — and its {@code INDIRECT/GROUP_BY} edges, on a <b>real</b> analyzed
 * Spark 4 plan rather than a mock.
 *
 * <p>The plan is built, never executed. {@code transformWithState} over a batch child constructs
 * the node without any streaming source — the node factory appends a dummy empty {@code
 * LocalRelation} as the right side when there is no initial state, and {@code
 * UnsupportedOperationChecker.checkForBatch} rejects only nodes whose {@code isStreaming} holds,
 * which a batch child never produces. No optimizer rule rewrites the node either, so {@code
 * optimizedPlan} is exactly what a listener would traverse on a real query.
 *
 * <p>The two shapes the implementation had to get right are pinned separately:
 *
 * <ul>
 *   <li><b>no initial state</b> — the right side is a dummy empty {@code LocalRelation} and must
 *       never surface as an input (the {@code BinaryNode} trap: {@code left = child}, {@code right
 *       = initialState}).
 *   <li><b>initial state</b> — the right side is the initial state's own plan and IS a real input
 *       for the boundary fan-in, while the grouping edges stay resolved against the left side
 *       only (a grouping against the initial state would double them).
 * </ul>
 *
 * <p>Dispatch in isolation ({@code isDefinedAt}/{@code apply} against a mock) is covered by
 * {@code TransformWithStateVisitorTest}; the AppendColumns grouping-key resolution mechanics are
 * shared with {@link MapGroupsColumnLineageTest} and documented on {@code TypedGroupByVisitor}.
 *
 * @see TypedLineageTestBase for the harness
 */
class TransformWithStateColumnLineageTest extends TypedLineageTestBase {

  /** A processor that is never run: the suite observes the plan, it does not execute it. */
  private static final class NoopProcessor extends StatefulProcessor<Integer, Row, Integer> {
    @Override
    public void init(OutputMode outputMode, TimeMode timeMode) {}

    @Override
    public scala.collection.Iterator<Integer> handleInputRows(
        Integer key, scala.collection.Iterator<Row> rows, TimerValues timerValues) {
      return emptyIterator();
    }
  }

  /** As {@link NoopProcessor}, for the initial-state overload. */
  private static final class NoopProcessorWithInitialState
      extends StatefulProcessorWithInitialState<Integer, Row, Integer, Row> {
    @Override
    public void init(OutputMode outputMode, TimeMode timeMode) {}

    @Override
    public scala.collection.Iterator<Integer> handleInputRows(
        Integer key, scala.collection.Iterator<Row> rows, TimerValues timerValues) {
      return emptyIterator();
    }

    @Override
    public void handleInitialState(Integer key, Row state, TimerValues timerValues) {}
  }

  private static <T> scala.collection.Iterator<T> emptyIterator() {
    return scala.collection.Iterator$.MODULE$.empty();
  }

  /** {@code groupByKey(row -> row.getInt(0) % 2).transformWithState(...)} over {@link #source()}. */
  private Dataset<Integer> transformWithStateReadingColumnA() {
    return source()
        .groupByKey((MapFunction<Row, Integer>) row -> row.getInt(0) % 2, Encoders.INT())
        .transformWithState(
            new NoopProcessor(), TimeMode.None(), OutputMode.Append(), Encoders.INT());
  }

  /**
   * The same grouping with an initial state drawn from a second source. The node's right side is
   * the initial state's own plan, whose {@code LogicalRDD} leaf carries exprIds distinct from the
   * grouped data's, so "left-only" versus "both sides" is observable in the edge counts.
   */
  private Dataset<Integer> transformWithStateWithInitialState() {
    return source()
        .groupByKey((MapFunction<Row, Integer>) row -> row.getInt(0) % 2, Encoders.INT())
        .transformWithState(
            new NoopProcessorWithInitialState(),
            TimeMode.None(),
            OutputMode.Append(),
            source().groupByKey((MapFunction<Row, Integer>) row -> row.getInt(0) % 2, Encoders.INT()),
            Encoders.INT(),
            Encoders.row(source().schema()));
  }

  @Test
  @DisplayName("groupByKey().transformWithState records INDIRECT/GROUP_BY, with or without the fan-in")
  void typedGroupByKeyTransformWithState() {
    Dataset<Integer> ds = transformWithStateReadingColumnA();

    // The node chain: TransformWithState over an AppendColumns, exactly the shape mapGroups
    // produces, wrapped by CatalystSerde.serialize. The trailing LocalRelation is the node's
    // right side — the dummy empty relation the factory appends when there is no initial state.
    // It is in the plan, and the assertions below are what pin that it never became an input:
    // three GROUP_BY edges (not six) and a four-input fan-in (not five).
    assertThat(planChain(ds))
        .isEqualTo(
            "SerializeFromObject>TransformWithState>AppendColumns>LogicalRDD>LocalRelation");

    // ONE dataset dependency, all three input columns hanging off it — the same signal as
    // mapGroups. The key lambda reads only "a"; the breadth is the deserialized schema (the
    // over-breadth TypedGroupByVisitor documents).
    List<String> groupByEdges = edges(ds);
    long dependencyId = soleDatasetDependencyId(groupByEdges, "INDIRECT/GROUP_BY");
    assertThat(groupByEdges)
        .containsExactlyInAnyOrder(
            "#" + dependencyId + "<-a INDIRECT/GROUP_BY/mask=false",
            "#" + dependencyId + "<-b INDIRECT/GROUP_BY/mask=false",
            "#" + dependencyId + "<-s INDIRECT/GROUP_BY/mask=false");

    // Facet level with the fan-in off: empty, exactly as for mapGroups — the GROUP_BY edge is a
    // dataset dependency and no field edge links the boundary back to the leaf without the fan-in.
    assertThat(facetFields(ds)).isEmpty();

    // With the fan-in on, the boundary supplies the missing field link and the GROUP_BY rides
    // along on the field, both transformations present per input field.
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
    Dataset<Integer> ds = transformWithStateReadingColumnA();

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
  @DisplayName("the dummy right side (hasInitialState=false) never surfaces as an input")
  void dummyInitialStateIsNotAnInput() {
    Dataset<Integer> ds = transformWithStateReadingColumnA();

    // A fan-in over the boundary reads the deserializers of the AppendColumns below the node.
    // If the dummy LocalRelation had been treated as an input its (empty) output would add
    // nothing — but any attempt to resolve a deserializer against it would have thrown, and the
    // visitor's reflective child() returning the left side is what keeps the descent honest.
    // The pin: the fan-in's input set is exactly the three real columns plus the synthetic
    // grouping key, so the TRANSFORMATION edges are four, not more.
    assertThat(edgesWithTypedBoundaryFanIn(ds, null))
        .filteredOn(edge -> edge.contains("TRANSFORMATION"))
        .hasSize(4);
  }

  @Test
  @DisplayName("hasInitialState=true: the initial state is a real input, the grouping stays left-only")
  void initialStateIsASecondInputNotAGrouping() {
    Dataset<Integer> ds = transformWithStateWithInitialState();

    // Both sides of the BinaryNode: left = the grouped data, right = the initial state's plan.
    assertThat(planChain(ds))
        .isEqualTo(
            "SerializeFromObject>TransformWithState"
                + ">AppendColumns>LogicalRDD"
                + ">AppendColumns>LogicalRDD");

    // The GROUP_BY edges resolve through the LEFT AppendColumns only — three edges, not six.
    // The initial state's exprIds are distinct from the grouped data's, so a grouping that
    // mistakenly resolved against the right side would double this count.
    assertThat(edges(ds)).filteredOn(edge -> edge.contains("GROUP_BY")).hasSize(3);

    // With the fan-in on, the initial state's columns ARE inputs — the state is derived from
    // them — so the boundary's input set grows from four (three columns + synthetic key) to
    // seven (three more from the initial state's own AppendColumns). The grouping signal is
    // unchanged.
    assertThat(edgesWithTypedBoundaryFanIn(ds, null))
        .filteredOn(edge -> edge.contains("GROUP_BY"))
        .hasSize(3);
    assertThat(edgesWithTypedBoundaryFanIn(ds, null))
        .filteredOn(edge -> edge.contains("TRANSFORMATION"))
        .hasSize(7);
  }
}