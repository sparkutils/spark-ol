/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.wrapper;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.transports.ProxyTransportWrapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ProxyTransportWrapperTest {

    ObjectMapper mapper = new ObjectMapper();
    ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
    URI producer = URI.create("producer");
    OpenLineage ol = new OpenLineage(producer);

    @BeforeEach
    void setup() {
        mapper.registerModule(new JavaTimeModule());
        mapper.setSerializationInclusion(Include.NON_NULL);
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
        mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
    }

    @Test
    void simple() {
        var evt1 = Fixtures.makeEventFixture(now, Fixtures.makeRunTagsFacet("foo", "baz"));
        var evt2 = Fixtures.makeEventFixture(now, Fixtures.makeRunTagsFacet("bar", "baz"));
        var r = ProxyTransportWrapper.mergeRuns(ol, evt1, evt2);
        assertEquals(r.getRun().getFacets().getTags().getTags().size(), 2);
    }
}
