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
 * The typed-boundary description names both sides of the hop — the input type(s) A and the output
 * type B — recovered from the plan (deserializer {@code dataType()} and {@code outputObjAttr}),
 * not from the lambda's bytecode.
 */
class TypedBoundaryTypeNamesTest extends TypedLineageTestBase {

  @Test
  @DisplayName("MapGroups names the key/value classes and the output class")
  void mapGroupsNamesBothSides() {
    Dataset<Integer> ds =
        source()
            .groupByKey((MapFunction<Row, Integer>) row -> row.getInt(0) % 2, Encoders.INT())
            .mapGroups(
                (MapGroupsFunction<Integer, Row, Integer>) (key, rows) -> key, Encoders.INT());

    List<String> described = describedEdgesWithTypedBoundaryFanIn(ds, null);
    assertThat(described).isNotEmpty();
    // Every fan-in edge carries the same description; inspect the first.
    String desc =
        described.get(0).substring(described.get(0).indexOf("/desc=") + "/desc=".length());

    // A = (key Integer, value Row), B = Integer. The AppendColumns node is also named because it
    // sits between MapGroups and the leaf and is part of the opaque chain.
    assertThat(desc).isEqualTo("MapGroups((Integer, Row) -> Integer), AppendColumns");
  }

  @Test
  @DisplayName("MapElements flags a same-type hop, and only a same-type hop")
  void mapElementsFlagsSameType() {
    // Row -> Row: the element type is preserved, so the hop is a field-level transform.
    Dataset<Row> sameType =
        source().map((MapFunction<Row, Row>) row -> row, Encoders.row(source().schema()));
    // Row -> String: a reshape/type change, so no same-type flag.
    Dataset<String> typeChange =
        source().map((MapFunction<Row, String>) row -> row.getString(2), Encoders.STRING());

    assertThat(descriptionOf(sameType)).isEqualTo("MapElements(Row -> Row), sameType");
    assertThat(descriptionOf(typeChange)).isEqualTo("MapElements(Row -> String)");
  }

  private String descriptionOf(Dataset<?> ds) {
    List<String> described = describedEdgesWithTypedBoundaryFanIn(ds, null);
    assertThat(described).isNotEmpty();
    return described.get(0).substring(described.get(0).indexOf("/desc=") + "/desc=".length());
  }
}
