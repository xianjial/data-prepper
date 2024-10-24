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
import java.util.stream.Collectors;

/**
 * An AggregateAction that will combine neptune stream entries to one output
 *
 * @since 1.3
 */
@DataPrepperPlugin(name = "neptune_aggregate", pluginType = AggregateAction.class,
        pluginConfigurationType = NeptuneAggregateAction.NeptuneAggregateActionConfig.class)
public class NeptuneAggregateAction implements AggregateAction {
    static final String OP_KEY_OP = "op";
    static final String OP_KEY_ENTITY_ID = "entity_id";
    static final String OP_KEY_ENTITY_TYPE = "entity_type";
    static final String OP_KEY_PREDICATES = "predicates";
    static final String OP_TYPE_ADD = "ADD";
    static final String OP_TYPE_DELETE = "DELETE";

    /**
     * During the handle event, we want to fill the GroupState with an object
     * looking like this so that it can be combined in the concludeGroup.
     * <p>
     * <p>
     * {
     * "[commitNum]": {
     * "[entity_id]": {
     * "[opNum]": {
     * "op": "ADD",
     * "commitNum": 3,
     * "opNum": 2,
     * "entity_id": "v://22c94d6b-f537-62f6-6407-f85fd4b59629",
     * "entity_type": [],
     * "document_type": "vertex",
     * "predicates": {
     * "name": [
     * {
     * "value": "dan",
     * "graph": null,
     * "language": null
     * }
     * ]
     * }
     * }
     * }
     * }
     * }
     */
    @Override
    public AggregateActionResponse handleEvent(final Event event, final AggregateActionInput aggregateActionInput) {
        final GroupState groupState = aggregateActionInput.getGroupState();

        final Integer commitNum = event.get("commit_num", Number.class).intValue();
        final Integer opNum = event.get("op_num", Number.class).intValue();
        final String entityId = event.get("entity_id", String.class);

        groupState.putIfAbsent(commitNum, new HashMap<>());
        final HashMap<String, HashMap<Integer, HashMap<String, Object>>> commitGroup = (HashMap<String, HashMap<Integer, HashMap<String, Object>>>) groupState.get(commitNum);

        assert commitGroup != null;

        commitGroup.putIfAbsent(entityId, new HashMap<>());
        HashMap<Integer, HashMap<String, Object>> currentEvent = commitGroup.get(entityId);

        assert currentEvent != null;

        currentEvent.putIfAbsent(opNum, getOperationFromEvent(event));
        HashMap<String, Object> currentOp = currentEvent.get(opNum);

        assert currentOp != null;

        return AggregateActionResponse.nullEventResponse();
    }

    /**
     * Retrieves the event in form of a hash map
     */
    private HashMap<String, Object> getOperationFromEvent(Event event) {
        final HashMap<String, Object> op = new HashMap<>();

        op.put(OP_KEY_OP, event.get(OP_KEY_OP, String.class));
        op.put(OP_KEY_ENTITY_ID, event.get(OP_KEY_ENTITY_ID, String.class));
        op.put(OP_KEY_ENTITY_TYPE, event.get(OP_KEY_ENTITY_TYPE, ArrayList.class));
        op.put(OP_KEY_PREDICATES, event.get(OP_KEY_PREDICATES, HashMap.class));

        return op;
    }

    /**
     * Retrieves the aggregated events. Used for the conclude action
     */
    private List<Event> getAggregatedEvents(GroupState groupState) {
        final ArrayList<NeptuneEventAggregated> events = new ArrayList<>();

        // Have to do this hack due to Java type erasure
        Map<Integer, HashMap<String, HashMap<Integer, HashMap<String, Object>>>> commitEntrySet = groupState.entrySet().stream()
                .filter(entry -> entry.getKey() instanceof Integer)
                .collect(Collectors.toMap(
                        entry -> (Integer) entry.getKey(),
                        entry -> (HashMap<String, HashMap<Integer, HashMap<String, Object>>>) entry.getValue()
                ));

        for (Map.Entry<Integer, HashMap<String, HashMap<Integer, HashMap<String, Object>>>> commitSet : commitEntrySet.entrySet()) {
            final LinkedHashMap<String, NeptuneEventAggregated> commitEvents = new LinkedHashMap<>();

            for (Map.Entry<String, HashMap<Integer, HashMap<String, Object>>> entitySet : commitSet.getValue().entrySet()) {
                NeptuneEventAggregated event = commitEvents.get(entitySet.getKey());
                if (event == null) {
                    event = new NeptuneEventAggregated(entitySet.getKey());
                    commitEvents.put(entitySet.getKey(), event);
                }

                for (Map.Entry<Integer, HashMap<String, Object>> opSet : entitySet.getValue().entrySet()) {
                    addOpToEvent(event, opSet.getValue());
                }
            }

            events.addAll(commitEvents.values());
        }

        return events.stream().map(NeptuneEventAggregated::toEvent).collect(Collectors.toList());
    }

    /**
     * Adds an operation to the event
     */
    private void addOpToEvent(NeptuneEventAggregated event, HashMap<String, Object> op) {
        final String opType = (String) op.get(OP_KEY_OP);
        final ArrayList<String> entityType = (ArrayList<String>) op.get(OP_KEY_ENTITY_TYPE);
        final HashMap<String, Object> predicates = (HashMap<String, Object>) op.get(OP_KEY_PREDICATES);

        if (!entityType.isEmpty()) {
            if (opType.equals(OP_TYPE_ADD)) {
                event.entityTypesToAdd.addAll(entityType);

            } else if (opType.equals(OP_TYPE_DELETE)) {
                event.entityTypesToDelete.addAll(entityType);
            }
        }

        if (!predicates.isEmpty()) {
            if (opType.equals(OP_TYPE_ADD)) {
                event.predicatesToAdd.add(predicates);

            } else if (opType.equals(OP_TYPE_DELETE)) {
                event.predicatesToDelete.add(predicates);
            }
        }
    }

    @JsonPropertyOrder
    @JsonClassDescription("The <code>neptune_aggregate</code> action combines neptune stream entries.")
    static class NeptuneAggregateActionConfig {
    }

    /**
     * Consolidates the group and return events in this format
     * <p>
     * <p>
     * {
     * "entity_id": "v2",
     * "entity_types_to_add": [
     * "Person"
     * ],
     * "entity_types_to_delete": [],
     * "predicates_to_delete": [],
     * "predicates_to_add": []
     * }
     */
    @Override
    public AggregateActionOutput concludeGroup(final AggregateActionInput aggregateActionInput) {
        GroupState groupState = aggregateActionInput.getGroupState();
        final List<Event> events = getAggregatedEvents(groupState);

        return new AggregateActionOutput(events);
    }
}

/**
 * Internal aggregate class for Neptune events
 */
class NeptuneEventAggregated {
    static final String EVENT_TYPE = "event";
    static final String EVENT_KEY_ENTITY_ID = "entity_id";
    static final String EVENT_KEY_ENTITY_TYPES_TO_ADD = "entity_types_to_add";
    static final String EVENT_KEY_ENTITY_TYPES_TO_DELETE = "entity_types_to_delete";
    static final String EVENT_KEY_PREDICATES_TO_ADD = "predicates_to_add";
    static final String EVENT_KEY_PREDICATES_TO_DELETE = "predicates_to_delete";

    public final String entityId;
    public final HashSet<String> entityTypesToAdd = new HashSet<>();
    public final HashSet<String> entityTypesToDelete = new HashSet<>();
    public final ArrayList<HashMap<String, Object>> predicatesToAdd = new ArrayList<>();
    public final ArrayList<HashMap<String, Object>> predicatesToDelete = new ArrayList<>();

    public NeptuneEventAggregated(String entityId) {
        this.entityId = entityId;
    }

    /**
     * @return A JacksonEvent representation
     */
    public Event toEvent() {
        return JacksonEvent.builder()
                .withEventType(EVENT_TYPE)
                .withData(Map.ofEntries(
                        Map.entry(EVENT_KEY_ENTITY_ID, this.entityId),
                        Map.entry(EVENT_KEY_ENTITY_TYPES_TO_ADD, this.entityTypesToAdd),
                        Map.entry(EVENT_KEY_ENTITY_TYPES_TO_DELETE, this.entityTypesToDelete),
                        Map.entry(EVENT_KEY_PREDICATES_TO_ADD, this.predicatesToAdd),
                        Map.entry(EVENT_KEY_PREDICATES_TO_DELETE, this.predicatesToDelete)
                ))
                .build();
    }
}