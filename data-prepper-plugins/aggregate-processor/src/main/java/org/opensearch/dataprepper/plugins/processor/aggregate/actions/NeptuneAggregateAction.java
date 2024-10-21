/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.plugins.processor.aggregate.*;

import java.util.*;

/**
 * An AggregateAction that will combine neptune stream entries to one output
 *
 * @since 1.3
 */
@DataPrepperPlugin(name = "neptune_aggregate", pluginType = AggregateAction.class,
        pluginConfigurationType = NeptuneAggregateAction.NeptuneAggregateActionConfig.class)
public class NeptuneAggregateAction implements AggregateAction {
    static final String EVENT_TYPE = "event";

    @Override
    public AggregateActionResponse handleEvent(final Event event, final AggregateActionInput aggregateActionInput) {
        final GroupState groupState = aggregateActionInput.getGroupState();
        if (groupState.isEmpty()) {
            groupState.putAll(getEmptyAggregatedEvent(event).toMap());
        }

        addEventToGroup(groupState, event);

        return AggregateActionResponse.nullEventResponse();
    }

    private void addEventToGroup(final GroupState groupState, final Event event) {
        final String opType = event.get("op", String.class);
        final ArrayList<String> entityType = event.get("entity_type", ArrayList.class);
        final HashMap predicates = event.get("predicates", HashMap.class);

        if (!entityType.isEmpty()) {
            if (opType.equals("ADD")) {
                ((ArrayList<String>) groupState.get("entity_types_to_add")).addAll(entityType);

            } else if (opType.equals("DELETE")) {
                ((ArrayList<String>) groupState.get("entity_types_to_delete")).addAll(entityType);
            }
        }

        if (!predicates.isEmpty()) {
            if (opType.equals("ADD")) {
                ((ArrayList) groupState.get("predicates_to_add")).add(predicates);

            } else if (opType.equals("DELETE")) {
                ((ArrayList) groupState.get("predicates_to_delete")).add(predicates);
            }
        }
    }

    private Event getEmptyAggregatedEvent(final Event event) {
        return JacksonEvent.builder()
                .withEventType("event")
                .withData(Map.ofEntries(
                        Map.entry("entity_id", event.getAsJsonString("entity_id")),
                        Map.entry("entity_types_to_add", new HashSet<String>()),
                        Map.entry("entity_types_to_delete", new HashSet<String>()),
                        Map.entry("predicates_to_add", new ArrayList<Map>()),
                        Map.entry("predicates_to_delete", new ArrayList<Map>())
                ))
                .build();
    }

    @JsonPropertyOrder
    @JsonClassDescription("The <code>neptune_aggregate</code> action combines neptune stream entries.")
    static class NeptuneAggregateActionConfig {
    }

    @Override
    public AggregateActionOutput concludeGroup(final AggregateActionInput aggregateActionInput) {
        final Event event = JacksonEvent.builder()
                .withEventType(EVENT_TYPE)
                .withData(aggregateActionInput.getGroupState())
                .build();
        return new AggregateActionOutput(List.of(event));
    }
}
