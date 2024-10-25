/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.plugins.processor.aggregate.*;
import org.opensearch.dataprepper.plugins.source.neptune.converter.OpenSearchDocument;
import org.opensearch.dataprepper.plugins.source.neptune.converter.OpenSearchDocumentPredicate;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class NeptuneAggregateActionTest {
    private AggregateAction neptuneAggregateAction;
    private Event neptuneStreamEvent1;
    private Event neptuneStreamEvent2;
    private Event neptuneStreamEvent3;
    private GroupState expectedGroupState;

    @BeforeEach
    void setup() {

        neptuneStreamEvent1 = JacksonEvent.builder()
                .withEventType("event")
                .withData(OpenSearchDocument
                        .builder()
                        .op("ADD")
                        .commitNum(1L)
                        .opNum(1L)
                        .entityId("v://22c94d6b-f537-62f6-6407-f85fd4b59629")
                        .documentType("vertex")
                        .entityType(Set.of("Person"))
                        .predicates(Map.of("name", Set.of(OpenSearchDocumentPredicate.builder().value("dan").graph("g1").language("en").build())))
                        .build())
                .build();

        neptuneStreamEvent2 = JacksonEvent.builder()
                .withEventType("event")
                .withData(OpenSearchDocument
                        .builder()
                        .op("ADD")
                        .commitNum(1L)
                        .opNum(2L)
                        .entityId("v://22c94d6b-f537-62f6-6407-f85fd4b59629")
                        .documentType("vertex")
                        .entityType(Set.of("Person"))
                        .predicates(Map.of("name", Set.of(OpenSearchDocumentPredicate.builder().value("dan").graph("g1").language("en").build())))
                        .build())
                .build();

        neptuneStreamEvent3 = JacksonEvent.builder()
                .withEventType("event")
                .withData(OpenSearchDocument
                        .builder()
                        .op("DELETE")
                        .commitNum(1L)
                        .opNum(3L)
                        .entityId("v://22c94d6b-f537-62f6-6407-f85fd4b59629")
                        .documentType("vertex")
                        .entityType(Set.of("Person"))
                        .predicates(Map.of("name", Set.of(OpenSearchDocumentPredicate.builder().value("dan").graph("g1").language("en").build())))
                        .build())
                .build();

        expectedGroupState = new AggregateActionTestUtils.TestGroupState();
    }

    private AggregateAction createObjectUnderTest() {
        return new NeptuneAggregateAction();
    }

    @Test
    void handleEvent_with_empty_groupState_returns_expected_AggregateResponse_and_modifies_groupState() {
        neptuneAggregateAction = createObjectUnderTest();
        final AggregateActionInput aggregateActionInput = new AggregateActionTestUtils.TestAggregateActionInput(Collections.emptyMap());
        final GroupState groupState = aggregateActionInput.getGroupState();

        final AggregateActionResponse aggregateActionResponse = neptuneAggregateAction.handleEvent(neptuneStreamEvent1, aggregateActionInput);

        expectedGroupState.put("events", Map.of(1L, Map.of(1L, OpenSearchDocument.fromEvent(neptuneStreamEvent1))));

        assertNull(aggregateActionResponse.getEvent());
        assertEquals(
                expectedGroupState,
                groupState
        );
    }

    @Test
    void handleEvent_with_non_empty_groupState_returns_expected_AggregateResponse_and_modifies_groupState() {
        neptuneAggregateAction = createObjectUnderTest();
        final AggregateActionInput aggregateActionInput = new AggregateActionTestUtils.TestAggregateActionInput(Collections.emptyMap());
        final GroupState groupState = aggregateActionInput.getGroupState();

        neptuneAggregateAction.handleEvent(neptuneStreamEvent1, aggregateActionInput);
        neptuneAggregateAction.handleEvent(neptuneStreamEvent2, aggregateActionInput);

        expectedGroupState.put("events", Map.of(1L, Map.of(1L, OpenSearchDocument.fromEvent(neptuneStreamEvent1), 2L, OpenSearchDocument.fromEvent(neptuneStreamEvent2))));

        assertEquals(
                expectedGroupState,
                groupState
        );
    }

    @Test
    void concludeGroup_with_two_add_and_one_delete() throws JsonProcessingException {
        neptuneAggregateAction = createObjectUnderTest();
        final AggregateActionInput aggregateActionInput = new AggregateActionTestUtils.TestAggregateActionInput(Collections.emptyMap());
        final GroupState groupState = aggregateActionInput.getGroupState();

        ObjectMapper objectMapper = new ObjectMapper();
        final List<JsonNode> expectedEvents = List.of(objectMapper.readTree("{\"predicates_to_delete\":[{\"value\":[{\"value\":null,\"graph\":null,\"language\":null}],\"key\":\"name\"}],\"entity_types_to_delete\":[\"Person\"],\"entity_id\":\"v://22c94d6b-f537-62f6-6407-f85fd4b59629\",\"predicates_to_add\":[{\"value\":[{\"value\":null,\"graph\":null,\"language\":null}],\"key\":\"name\"},{\"value\":[{\"value\":null,\"graph\":null,\"language\":null}],\"key\":\"name\"}],\"entity_types_to_add\":[\"Person\"]}"));

        final TreeMap<Long, TreeMap<Long, OpenSearchDocument>> inputEvents = new TreeMap<>(
                Map.of(1L, new TreeMap<>(Map.of(
                        1L, OpenSearchDocument.fromEvent(neptuneStreamEvent1),
                        2L, OpenSearchDocument.fromEvent(neptuneStreamEvent2),
                        3L, OpenSearchDocument.fromEvent(neptuneStreamEvent3)
                )))
        );

        groupState.put("events", inputEvents);

        final AggregateActionOutput output = neptuneAggregateAction.concludeGroup(aggregateActionInput);

        final List<Event> actualEvents = output.getEvents();

        assertEquals(expectedEvents.size(), actualEvents.size());

        for (int i = 0; i < expectedEvents.size(); i++) {
            assertEquals(expectedEvents.get(i), objectMapper.readTree(actualEvents.get(i).toJsonString()));
        }
    }

    @Test
    void concludeGroup_missing_sequence() {
        neptuneAggregateAction = createObjectUnderTest();
        final AggregateActionInput aggregateActionInput = new AggregateActionTestUtils.TestAggregateActionInput(Collections.emptyMap());
        final GroupState groupState = aggregateActionInput.getGroupState();

        final TreeMap<Long, TreeMap<Long, OpenSearchDocument>> inputEvents = new TreeMap<>(
                Map.of(1L, new TreeMap<>(Map.of(
                        1L, OpenSearchDocument.fromEvent(neptuneStreamEvent1),
                        3L, OpenSearchDocument.fromEvent(neptuneStreamEvent3)
                )))
        );

        groupState.put("events", inputEvents);

        assertThrows(AssertionError.class, () -> neptuneAggregateAction.concludeGroup(aggregateActionInput));
    }
}
