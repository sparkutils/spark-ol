/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageContext;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.plans.logical.LeafNode;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata$;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeAll;
import org.mockito.invocation.InvocationOnMock;

/**
 * Shared observation harness for the UDF / typed-operation column-lineage suites.
 *
 * <p><b>Two observation levels</b> are used, because they disagree and the disagreement matters:
 *
 * <ul>
 *   <li><b>edge level</b> — what {@link ExpressionDependencyCollector} pushes into the builder.
 *   <li><b>facet level</b> — what survives into the {@code columnLineage} facet fields. An output
 *       attribute with no path to a registered input contributes no facet field even when edges
 *       exist elsewhere in the plan.
 * </ul>
 *
 * <p><b>Two configuration dimensions</b> are needed by more than one suite: {@code withDescription}
 * renders {@link TransformationInfo#getDescription()} (the field a UDF edge populates and an ordinary
 * expression leaves empty), and an override {@link SparkOpenLineageConfig} turns on {@code
 * typedBoundaryFanInEnabled}, which ships {@code false}.
 *
 * <p>Verified against Spark 4.0.0 / Scala 2.13 and Spark 4.1 / Scala 2.13.18.
 */
abstract class TypedLineageTestBase {

  protected static final DatasetIdentifier SOURCE = new DatasetIdentifier("src", "test");

  private static SparkSession spark;

  /**
   * One session for the whole suite family, created on first use and deliberately never stopped. It
   * is local[1] and dies with the JVM, so leaving it running beats tearing it down and rebuilding it
   * once per subclass.
   */
  @BeforeAll
  static synchronized void startSpark() {
    if (spark != null) {
      return;
    }
    spark =
        SparkSession.builder()
            .master("local[1]")
            .appName("udf-typed-op-characterisation")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.ui.enabled", "false")
            .getOrCreate();
    spark.udf().register("plus1", (Integer x) -> x == null ? null : x + 1, DataTypes.IntegerType);
    spark
        .udf()
        .register(
            "addTwo",
            (Integer x, Integer y) -> (x == null || y == null) ? null : x + y,
            DataTypes.IntegerType);
  }

  protected static SparkSession spark() {
    return spark;
  }

  // ---------------------------------------------------------------------------
  // fixtures
  // ---------------------------------------------------------------------------

  /**
   * An RDD-backed leaf with three genuinely distinct columns.
   *
   * <p>Deliberately not {@code createDataFrame(List)} (which folds to a {@code LocalRelation} and
   * lets the optimizer evaluate the UDF away, leaving nothing to traverse) and deliberately not
   * {@code spark.range} (whose single {@code id} column makes multi-argument fan-in unobservable —
   * every argument resolves back to the same input field). File-based sources are also avoided: a
   * {@code delta-spark} artifact on this module's test classpath is built against a newer Spark and
   * blows up {@code DataSource} resolution under Spark 4.0.0.
   */
  protected Dataset<Row> source() {
    StructType schema =
        new StructType()
            .add("a", DataTypes.IntegerType)
            .add("b", DataTypes.IntegerType)
            .add("s", DataTypes.StringType);
    return spark.createDataFrame(
        new JavaSparkContext(spark.sparkContext())
            .parallelize(Arrays.asList(RowFactory.create(1, 2, "x"), RowFactory.create(3, 4, "y"))),
        schema);
  }

  /** A wide RDD-backed leaf with {@code width} distinct integer columns {@code c0..c<width-1>}. */
  protected Dataset<Row> wideSource(int width) {
    StructType schema = new StructType();
    Object[] values = new Object[width];
    for (int i = 0; i < width; i++) {
      schema = schema.add("c" + i, DataTypes.IntegerType);
      values[i] = i;
    }
    return spark.createDataFrame(
        new JavaSparkContext(spark.sparkContext())
            .parallelize(Collections.singletonList(RowFactory.create(values))),
        schema);
  }

  protected static AttributeReference attribute(String name, long id) {
    return new AttributeReference(
        name,
        IntegerType$.MODULE$,
        false,
        Metadata$.MODULE$.empty(),
        ExprId.apply(id),
        ScalaConversionUtils.asScalaSeqEmpty());
  }

  // ---------------------------------------------------------------------------
  // plan observation
  // ---------------------------------------------------------------------------

  /** The plan-node chain, pre-order, as a {@code >}-separated string of simple class names. */
  protected static String planChain(LogicalPlan plan) {
    StringBuilder chain = new StringBuilder();
    plan.foreach(
        op -> {
          if (chain.length() > 0) {
            chain.append('>');
          }
          chain.append(op.getClass().getSimpleName());
          return scala.runtime.BoxedUnit.UNIT;
        });
    return chain.toString();
  }

  protected static String planChain(Dataset<?> ds) {
    return planChain(ds.queryExecution().optimizedPlan());
  }

  /** Maps every attribute id appearing anywhere in the plan to its name, for readable assertions. */
  protected static Map<Long, String> attributeNames(LogicalPlan plan) {
    Map<Long, String> names = new HashMap<>();
    plan.foreach(
        op -> {
          ScalaConversionUtils.<Attribute>fromSeq(op.output())
              .forEach(attr -> names.putIfAbsent(attr.exprId().id(), attr.name()));
          return scala.runtime.BoxedUnit.UNIT;
        });
    return names;
  }

  // ---------------------------------------------------------------------------
  // config
  // ---------------------------------------------------------------------------

  /** The shipped defaults, with dataset-level lineage off so facet output stays field-scoped. */
  protected static SparkOpenLineageConfig defaultConfig() {
    SparkOpenLineageConfig config = new SparkOpenLineageConfig();
    config.getColumnLineageConfig().setDatasetLineageEnabled(false);
    return config;
  }

  /**
   * The typed-boundary fan-in turned ON, with the width cap set to {@code maxEdges}. Pass {@code
   * null} to keep the configured default cap.
   */
  protected static SparkOpenLineageConfig typedBoundaryFanInConfig(Integer maxEdges) {
    SparkOpenLineageConfig config = defaultConfig();
    config.getColumnLineageConfig().setTypedBoundaryFanInEnabled(true);
    if (maxEdges != null) {
      config.getColumnLineageConfig().setTypedBoundaryFanInMaxEdges(maxEdges);
    }
    return config;
  }

  // ---------------------------------------------------------------------------
  // edge-level observation
  // ---------------------------------------------------------------------------

  /**
   * Edge-level observation: every {@code addDependency} the collector makes, rendered as {@code
   * <outName><-<inName> TYPE/SUBTYPE/mask=<bool>}. Output ids that are not attributes in the plan
   * (intermediate aliases) render as {@code #<id>}.
   */
  protected List<String> edges(Dataset<?> ds) {
    LogicalPlan plan = ds.queryExecution().optimizedPlan();
    return edges(plan, attributeNames(plan), null, false);
  }

  protected List<String> edges(LogicalPlan plan, Map<Long, String> names) {
    return edges(plan, names, null, false);
  }

  /**
   * As {@link #edges(Dataset)}, but also renders {@link TransformationInfo#getDescription()} as a
   * trailing {@code /desc=<...>} segment.
   *
   * <p>Kept as a separate rendering rather than folded into {@link #edges(Dataset)} so that the
   * control assertions stay byte-for-byte as the baseline captured them: the description is the one
   * field a UDF edge populates and an ordinary expression leaves empty, so including it everywhere
   * would churn every unrelated expectation.
   */
  protected List<String> describedEdges(Dataset<?> ds) {
    LogicalPlan plan = ds.queryExecution().optimizedPlan();
    return edges(plan, attributeNames(plan), null, true);
  }

  protected List<String> describedEdges(LogicalPlan plan, Map<Long, String> names) {
    return edges(plan, names, null, true);
  }

  /**
   * As {@link #edges(Dataset)}, but with the typed-boundary fan-in enabled and the width cap set to
   * {@code maxEdges}. Pass {@code null} for {@code maxEdges} to keep the configured default.
   */
  protected List<String> edgesWithTypedBoundaryFanIn(Dataset<?> ds, Integer maxEdges) {
    LogicalPlan plan = ds.queryExecution().optimizedPlan();
    return edges(plan, attributeNames(plan), typedBoundaryFanInConfig(maxEdges), false);
  }

  protected List<String> edges(
      LogicalPlan plan,
      Map<Long, String> names,
      SparkOpenLineageConfig config,
      boolean withDescription) {
    ColumnLevelLineageBuilder builder = mock(ColumnLevelLineageBuilder.class);
    ColumnLevelLineageContext context = mock(ColumnLevelLineageContext.class);
    OpenLineageContext olContext = mock(OpenLineageContext.class);
    when(context.getBuilder()).thenReturn(builder);
    when(context.getOlContext()).thenReturn(olContext);
    when(olContext.getColumnLevelLineageVisitors()).thenReturn(Collections.emptyList());
    // Operator visitors only receive the builder, so config reaches them through it.
    when(builder.getContext()).thenReturn(olContext);
    if (config != null) {
      when(olContext.getOpenLineageConfig()).thenReturn(config);
    }

    List<String> collected = new ArrayList<>();
    org.mockito.Mockito.doAnswer(
            (InvocationOnMock invocation) -> {
              Object[] args = invocation.getArguments();
              ExprId out = (ExprId) args[0];
              ExprId in = (ExprId) args[1];
              TransformationInfo info =
                  args.length > 2 ? (TransformationInfo) args[2] : TransformationInfo.identity();
              collected.add(
                  render(names, out)
                      + "<-"
                      + render(names, in)
                      + " "
                      + info.getType()
                      + "/"
                      + info.getSubType()
                      + "/mask="
                      + info.getMasking()
                      + (withDescription ? "/desc=" + info.getDescription() : ""));
              return null;
            })
        .when(builder)
        .addDependency(
            org.mockito.ArgumentMatchers.any(ExprId.class),
            org.mockito.ArgumentMatchers.any(ExprId.class),
            org.mockito.ArgumentMatchers.any(TransformationInfo.class));
    org.mockito.Mockito.doAnswer(
            (InvocationOnMock invocation) -> {
              Object[] args = invocation.getArguments();
              collected.add(
                  render(names, (ExprId) args[0])
                      + "<-"
                      + render(names, (ExprId) args[1])
                      + " DIRECT/IDENTITY/mask=false"
                      + (withDescription ? "/desc=" : ""));
              return null;
            })
        .when(builder)
        .addDependency(
            org.mockito.ArgumentMatchers.any(ExprId.class),
            org.mockito.ArgumentMatchers.any(ExprId.class));

    ExpressionDependencyCollector.collect(context, plan);
    return collected;
  }

  protected static String render(Map<Long, String> names, ExprId id) {
    return names.getOrDefault(id.id(), "#" + id.id());
  }

  /**
   * The same edge list as {@link #edges(Dataset)} but with the output side stripped, for operators
   * such as {@code Filter}/{@code TypedFilter} whose output id is a freshly minted synthetic
   * dataset-dependency {@link ExprId} that changes between runs and carries no meaning.
   */
  protected List<String> edgesWithoutOutputId(Dataset<?> ds) {
    return edges(ds).stream()
        .map(edge -> edge.substring(edge.indexOf("<-")))
        .collect(Collectors.toList());
  }

  /** The distinct synthetic (non-attribute) output ids appearing in the edge list. */
  protected List<String> syntheticOutputIds(Dataset<?> ds) {
    return edges(ds).stream()
        .map(edge -> edge.substring(0, edge.indexOf("<-")))
        .filter(out -> out.startsWith("#"))
        .distinct()
        .collect(Collectors.toList());
  }

  /**
   * The id of the single dataset dependency that the edges matching {@code subtype} hang off,
   * failing if they do not all share one.
   */
  protected static long soleDatasetDependencyId(List<String> edges, String subtype) {
    List<Long> ids =
        edges.stream()
            .filter(edge -> edge.contains(subtype))
            .map(edge -> Long.parseLong(edge.substring(1, edge.indexOf("<-"))))
            .distinct()
            .collect(Collectors.toList());
    assertThat(ids).hasSize(1);
    return ids.get(0);
  }

  // ---------------------------------------------------------------------------
  // facet-level observation
  // ---------------------------------------------------------------------------

  /**
   * Facet-level observation: the {@code columnLineage} facet fields, rendered as {@code
   * <outField><-<inField> TYPE/SUBTYPE/mask=<bool>}.
   *
   * <p>{@link InputFieldsCollector} is stood in for rather than invoked, because it resolves a
   * {@link DatasetIdentifier} from the concrete relation type and an RDD-backed leaf has none. Every
   * {@link LeafNode} output attribute is registered against a single synthetic source dataset, which
   * is exactly what the real collector would do for a file/table relation and keeps these tests
   * focused on traversal rather than on dataset identification.
   */
  protected List<String> facetFields(Dataset<?> ds) {
    return facetFields(ds, null, false);
  }

  protected List<String> facetFields(Dataset<?> ds, SparkOpenLineageConfig overrideConfig) {
    return facetFields(ds, overrideConfig, false);
  }

  /** As {@link #facetFields(Dataset)}, but also renders the transformation description. */
  protected List<String> describedFacetFields(Dataset<?> ds) {
    return facetFields(ds, null, true);
  }

  /** As {@link #facetFields(Dataset)} but with the typed-boundary fan-in enabled. */
  protected List<String> facetFieldsWithTypedBoundaryFanIn(Dataset<?> ds) {
    return facetFields(ds, typedBoundaryFanInConfig(null), false);
  }

  protected List<String> facetFields(
      Dataset<?> ds, SparkOpenLineageConfig overrideConfig, boolean withDescription) {
    LogicalPlan plan = ds.queryExecution().optimizedPlan();
    OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);

    OpenLineage.SchemaDatasetFacet schema =
        openLineage.newSchemaDatasetFacet(
            Arrays.stream(ds.schema().fields())
                .map(
                    field ->
                        openLineage
                            .newSchemaDatasetFacetFieldsBuilder()
                            .name(field.name())
                            .type(field.dataType().typeName())
                            .build())
                .collect(Collectors.toList()));

    OpenLineageContext olContext = mock(OpenLineageContext.class);
    SparkOpenLineageConfig config = overrideConfig != null ? overrideConfig : defaultConfig();
    when(olContext.getOpenLineage()).thenReturn(openLineage);
    when(olContext.getOpenLineageConfig()).thenReturn(config);
    when(olContext.getColumnLevelLineageVisitors()).thenReturn(Collections.emptyList());

    ColumnLevelLineageBuilder builder = new ColumnLevelLineageBuilder(schema, olContext);
    ColumnLevelLineageContext context = mock(ColumnLevelLineageContext.class);
    when(context.getBuilder()).thenReturn(builder);
    when(context.getOlContext()).thenReturn(olContext);
    when(context.getEvent()).thenReturn(mock(SparkListenerEvent.class));

    OutputFieldsCollector.collect(context, plan);
    ExpressionDependencyCollector.collect(context, plan);
    plan.foreach(
        op -> {
          if (op instanceof LeafNode) {
            ScalaConversionUtils.<Attribute>fromSeq(op.output()).stream()
                .filter(attr -> attr instanceof AttributeReference)
                .forEach(attr -> builder.addInput(attr.exprId(), SOURCE, attr.name()));
          }
          return scala.runtime.BoxedUnit.UNIT;
        });

    List<String> rendered = new ArrayList<>();
    builder
        .buildFields(false)
        .getAdditionalProperties()
        .forEach(
            (outField, inputs) ->
                inputs
                    .getInputFields()
                    .forEach(
                        input ->
                            input
                                .getTransformations()
                                .forEach(
                                    t ->
                                        rendered.add(
                                            outField
                                                + "<-"
                                                + input.getField()
                                                + " "
                                                + t.getType()
                                                + "/"
                                                + t.getSubtype()
                                                + "/mask="
                                                + t.getMasking()
                                                + (withDescription
                                                    ? "/desc=" + t.getDescription()
                                                    : "")))));
    return rendered;
  }
}
