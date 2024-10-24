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
    static final String GROUP_KEY_EVENTS = "events";

    /**
     * During the handle event, we want to fill the GroupState with an object
     * looking like this so that it can be combined in the concludeGroup.
     * <p>
     * <p>
     * {
     * "[commitNum]": {
     * "[opNum]": {
     * "op": "ADD",
     * "entity_id": "v://22c94d6b-f537-62f6-6407-f85fd4b59629",
     * "entity_type": [],
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
     */
    @Override
    public AggregateActionResponse handleEvent(final Event event, final AggregateActionInput aggregateActionInput) {
        // Retrieve the group state
        final GroupState groupState = aggregateActionInput.getGroupState();

        // Get the important data from the event
        final Integer commitNum = event.get("commit_num", Number.class).intValue();
        final Integer opNum = event.get("op_num", Number.class).intValue();

        // If not exists, set the "events" key with a TreeMap
        groupState.putIfAbsent(GROUP_KEY_EVENTS, new TreeMap<>());
        final TreeMap<Integer, TreeMap<Integer, NeptuneEventSimple>> eventsMap = (TreeMap<Integer, TreeMap<Integer, NeptuneEventSimple>>) groupState.get(GROUP_KEY_EVENTS);
        assert eventsMap != null;

        // If not exists, set the commit with a TreeMap
        eventsMap.putIfAbsent(commitNum, new TreeMap<>());
        final TreeMap<Integer, NeptuneEventSimple> commitGroup = (TreeMap<Integer, NeptuneEventSimple>) groupState.get(commitNum);
        assert commitGroup != null;

        // If not exists, set the op as NeptuneEventSimple
        commitGroup.putIfAbsent(opNum, new NeptuneEventSimple(event));
        NeptuneEventSimple currentOp = commitGroup.get(opNum);
        assert currentOp != null;

        return AggregateActionResponse.nullEventResponse();
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

    /**
     * Asserts the sequence of a set
     */
    private void assertSequence(List<Integer> intList) {
        for (int i = 0; i < intList.size() - 1; i++) {
            int diff = intList.get(i + 1) - intList.get(i);
            assert diff == 1;
        }
    }

    /**
     * Retrieves the aggregated events. Used for the conclude action
     */
    private List<Event> getAggregatedEvents(GroupState groupState) {
        final ArrayList<NeptuneEventAggregated> events = new ArrayList<>();

        final TreeMap<Integer, TreeMap<Integer, NeptuneEventSimple>> eventsMap = (TreeMap<Integer, TreeMap<Integer, NeptuneEventSimple>>) groupState.get(GROUP_KEY_EVENTS);

        assertSequence(new ArrayList<>(eventsMap.keySet()));

        for (Map.Entry<Integer, TreeMap<Integer, NeptuneEventSimple>> commitSet : eventsMap.entrySet()) {

            assertSequence(new ArrayList<>(commitSet.getValue().keySet()));

            // We do this as a sub list to keep commits grouped and restricted
            final LinkedHashMap<String, NeptuneEventAggregated> commitEvents = new LinkedHashMap<>();

            for (Map.Entry<Integer, NeptuneEventSimple> opSet : commitSet.getValue().entrySet()) {
                NeptuneEventSimple op = opSet.getValue();

                NeptuneEventAggregated event = commitEvents.get(op.entityId);
                if (event == null) {
                    event = new NeptuneEventAggregated(op.entityId);
                    commitEvents.put(op.entityId, event);
                }

                event.consumeNeptuneEventSimple(op);
            }

            events.addAll(commitEvents.values());
        }

        return events.stream().map(NeptuneEventAggregated::toEvent).collect(Collectors.toList());
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
    static final String OP_TYPE_ADD = "ADD";
    static final String OP_TYPE_DELETE = "DELETE";

    public final String entityId;
    public final HashSet<String> entityTypesToAdd = new HashSet<>();
    public final HashSet<String> entityTypesToDelete = new HashSet<>();
    public final ArrayList<HashMap<String, Object>> predicatesToAdd = new ArrayList<>();
    public final ArrayList<HashMap<String, Object>> predicatesToDelete = new ArrayList<>();

    public NeptuneEventAggregated(String entityId) {
        this.entityId = entityId;
    }

    public void consumeNeptuneEventSimple(NeptuneEventSimple event) {
        if (!event.entityType.isEmpty()) {
            if (event.op.equals(OP_TYPE_ADD)) {
                this.entityTypesToAdd.addAll(event.entityType);

            } else if (event.op.equals(OP_TYPE_DELETE)) {
                this.entityTypesToDelete.addAll(event.entityType);
            }
        }

        if (!event.predicates.isEmpty()) {
            if (event.op.equals(OP_TYPE_ADD)) {
                this.predicatesToAdd.add(event.predicates);

            } else if (event.op.equals(OP_TYPE_DELETE)) {
                this.predicatesToDelete.add(event.predicates);
            }
        }
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

/**
 * Internal simplified class for Neptune events
 */
class NeptuneEventSimple {
    static final String EVENT_KEY_OP = "op";
    static final String EVENT_KEY_ENTITY_ID = "entity_id";
    static final String EVENT_KEY_ENTITY_TYPE = "entity_type";
    static final String EVENT_KEY_PREDICATES = "predicates";

    public final String op;
    public final String entityId;
    public final ArrayList<String> entityType;
    public final HashMap<String, Object> predicates;

    public NeptuneEventSimple(Event event) {
        this.op = event.get(EVENT_KEY_OP, String.class);
        this.entityId = event.get(EVENT_KEY_ENTITY_ID, String.class);
        this.entityType = event.get(EVENT_KEY_ENTITY_TYPE, ArrayList.class);
        this.predicates = event.get(EVENT_KEY_PREDICATES, HashMap.class);
    }
}