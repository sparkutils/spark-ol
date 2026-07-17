package io.openlineage.client.wrapper;

import io.openlineage.client.OpenLineage;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class Fixtures {
    public static OpenLineage.TagsRunFacet makeRunTagsFacet(String key, String value) {
        URI producer = URI.create("producer");
        OpenLineage ol = new OpenLineage(producer);
        var tag = ol.newTagsRunFacetFieldsBuilder().key(key).value(value).source("CONFIG").build();
        var tags = new ArrayList<OpenLineage.TagsRunFacetFields>();
        tags.add(tag);
        return ol.newTagsRunFacet(tags);
    }
    public static OpenLineage.RunEvent makeEventFixture(
            ZonedDateTime now,
            OpenLineage.TagsRunFacet tags
    ) {
        URI producer = URI.create("producer");
        OpenLineage ol = new OpenLineage(producer);
        UUID runId = UUID.randomUUID();
        UUID parentId = UUID.randomUUID();
        UUID rootParentId = UUID.randomUUID();


        OpenLineage.RunFacets runFacets =
                ol.newRunFacetsBuilder()
                        .nominalTime(
                                ol.newNominalTimeRunFacetBuilder()
                                        .nominalStartTime(now)
                                        .nominalEndTime(now)
                                        .build())
                        .tags(tags)
                        .parent(
                                ol.newParentRunFacetBuilder()
                                        .job(ol.newParentRunFacetJob("parent-namespace", "parent-name"))
                                        .run(ol.newParentRunFacetRun(parentId))
                                        .root(
                                                ol.newParentRunFacetRoot(
                                                        ol.newRootRun(rootParentId),
                                                        ol.newRootJob("root-namespace", "root-job-name")))
                                        .build())
                        .build();
        OpenLineage.Run run = ol.newRunBuilder().runId(runId).facets(runFacets).build();
        String name = "jobName";
        String namespace = "namespace";
        OpenLineage.JobFacets jobFacets = ol.newJobFacetsBuilder().build();
        OpenLineage.Job job = ol.newJobBuilder().namespace(namespace).name(name).facets(jobFacets).build();

        List<OpenLineage.InputDataset> inputs =
                Arrays.asList(
                        ol.newInputDatasetBuilder()
                                .namespace("ins")
                                .name("input")
                                .facets(
                                        ol.newDatasetFacetsBuilder()
                                                .version(ol.newDatasetVersionDatasetFacet("input-version"))
                                                .build())
                                .inputFacets(
                                        ol.newInputDatasetInputFacetsBuilder()
                                                .put(
                                                        "subset",
                                                        ol.newInputSubsetInputDatasetFacetBuilder()
                                                                .inputCondition(
                                                                        ol.newLocationSubsetConditionBuilder()
                                                                                .locations(
                                                                                        Arrays.asList("s3://bucket/key1", "s3://bucket/key2"))
                                                                                .build())
                                                                .build())
                                                .dataQualityMetrics(
                                                        ol.newDataQualityMetricsInputDatasetFacetBuilder()
                                                                .rowCount(10L)
                                                                .bytes(20L)
                                                                .fileCount(5L)
                                                                .columnMetrics(
                                                                        ol.newDataQualityMetricsInputDatasetFacetColumnMetricsBuilder()
                                                                                .put(
                                                                                        "mycol",
                                                                                        ol.newDataQualityMetricsInputDatasetFacetColumnMetricsAdditionalBuilder()
                                                                                                .count(10D)
                                                                                                .distinctCount(10L)
                                                                                                .max(30D)
                                                                                                .min(5D)
                                                                                                .nullCount(1L)
                                                                                                .sum(3000D)
                                                                                                .quantiles(
                                                                                                        ol.newDataQualityMetricsInputDatasetFacetColumnMetricsAdditionalQuantilesBuilder()
                                                                                                                .put("25", 52D)
                                                                                                                .build())
                                                                                                .build())
                                                                                .build())
                                                                .build())
                                                .build())
                                .build());

        OpenLineage.SchemaDatasetFacet schemaFacet =
                ol.newSchemaDatasetFacetBuilder()
                        .fields(
                                Arrays.asList(
                                        ol.newSchemaDatasetFacetFieldsBuilder().name("user_id").type("int64").build(),
                                        ol.newSchemaDatasetFacetFieldsBuilder()
                                                .name("phones")
                                                .type("array")
                                                .description("List of phone numbers")
                                                .fields(
                                                        Arrays.asList(
                                                                ol.newSchemaDatasetFacetFieldsBuilder()
                                                                        .name("_element")
                                                                        .type("string")
                                                                        .build()))
                                                .build(),
                                        ol.newSchemaDatasetFacetFieldsBuilder()
                                                .name("addresses")
                                                .type("struct")
                                                .description("Has customer completed activation process")
                                                .fields(
                                                        Arrays.asList(
                                                                ol.newSchemaDatasetFacetFieldsBuilder()
                                                                        .name("type")
                                                                        .type("string")
                                                                        .description("Address type, g.e. home, work, etc.")
                                                                        .build(),
                                                                ol.newSchemaDatasetFacetFieldsBuilder()
                                                                        .name("country")
                                                                        .type("string")
                                                                        .description("Country name")
                                                                        .build(),
                                                                ol.newSchemaDatasetFacetFieldsBuilder()
                                                                        .name("zip")
                                                                        .type("string")
                                                                        .description("Zip code")
                                                                        .build(),
                                                                ol.newSchemaDatasetFacetFieldsBuilder()
                                                                        .name("state")
                                                                        .type("string")
                                                                        .description("State name")
                                                                        .build(),
                                                                ol.newSchemaDatasetFacetFieldsBuilder()
                                                                        .name("street")
                                                                        .type("string")
                                                                        .description("Street name")
                                                                        .build()))
                                                .build(),
                                        ol.newSchemaDatasetFacetFieldsBuilder()
                                                .name("custom_properties")
                                                .type("map")
                                                .fields(
                                                        Arrays.asList(
                                                                ol.newSchemaDatasetFacetFieldsBuilder()
                                                                        .name("key")
                                                                        .type("string")
                                                                        .build(),
                                                                ol.newSchemaDatasetFacetFieldsBuilder()
                                                                        .name("value")
                                                                        .type("union")
                                                                        .fields(
                                                                                Arrays.asList(
                                                                                        ol.newSchemaDatasetFacetFieldsBuilder()
                                                                                                .name("_0")
                                                                                                .type("string")
                                                                                                .build(),
                                                                                        ol.newSchemaDatasetFacetFieldsBuilder()
                                                                                                .name("_1")
                                                                                                .type("int64")
                                                                                                .build()))
                                                                        .build()))
                                                .build()))
                        .build();

        List<OpenLineage.OutputDataset> outputs =
                Arrays.asList(
                        ol.newOutputDatasetBuilder()
                                .namespace("ons")
                                .name("output")
                                .facets(
                                        ol.newDatasetFacetsBuilder()
                                                .version(ol.newDatasetVersionDatasetFacet("output-version"))
                                                .schema(schemaFacet)
                                                .build())
                                .outputFacets(
                                        ol.newOutputDatasetOutputFacetsBuilder()
                                                .outputStatistics(
                                                        ol.newOutputStatisticsOutputDatasetFacetBuilder()
                                                                .rowCount(10L)
                                                                .size(20L)
                                                                .fileCount(5L)
                                                                .build())
                                                .build())
                                .build());

        OpenLineage.RunEvent runStateUpdate =
                ol.newRunEventBuilder()
                        .eventType(OpenLineage.RunEvent.EventType.START)
                        .eventTime(now)
                        .run(run)
                        .job(job)
                        .inputs(inputs)
                        .outputs(outputs)
                        .build();
        return runStateUpdate;
    }
}
