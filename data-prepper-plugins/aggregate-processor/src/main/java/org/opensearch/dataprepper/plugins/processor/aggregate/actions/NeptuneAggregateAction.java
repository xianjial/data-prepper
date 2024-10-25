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
import org.opensearch.dataprepper.plugins.source.neptune.converter.OpenSearchDocument;
import org.opensearch.dataprepper.plugins.source.neptune.converter.OpenSearchDocumentPredicate;

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

        OpenSearchDocument parsedEvent = OpenSearchDocument.fromEvent(event);

        // If not exists, set the "events" key with a TreeMap
        groupState.putIfAbsent(GROUP_KEY_EVENTS, new TreeMap<>());
        final TreeMap<Long, TreeMap<Long, OpenSearchDocument>> eventsMap = (TreeMap<Long, TreeMap<Long, OpenSearchDocument>>) groupState.get(GROUP_KEY_EVENTS);
        assert eventsMap != null;

        // If not exists, set the commit with a TreeMap
        eventsMap.putIfAbsent(parsedEvent.getCommitNum(), new TreeMap<>());
        final TreeMap<Long, OpenSearchDocument> commitGroup = eventsMap.get(parsedEvent.getCommitNum());
        assert commitGroup != null;

        // If not exists, set the op as OpenSearchDocument
        commitGroup.putIfAbsent(parsedEvent.getOpNum(), parsedEvent);
        OpenSearchDocument currentOp = commitGroup.get(parsedEvent.getOpNum());
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

        final TreeMap<Long, TreeMap<Long, OpenSearchDocument>> eventsMap = (TreeMap<Long, TreeMap<Long, OpenSearchDocument>>) groupState.get(GROUP_KEY_EVENTS);

        final List<Event> events = getAggregatedEvents(eventsMap);

        return new AggregateActionOutput(events);
    }

    /**
     * Asserts the sequence of a set
     */
    private void assertSequence(List<Long> sequenceList) {
        for (int i = 0; i < sequenceList.size() - 1; i++) {
            long diff = sequenceList.get(i + 1) - sequenceList.get(i);
            assert diff == 1;
        }
    }

    /**
     * Retrieves the aggregated events. Used for the conclude action
     */
    private List<Event> getAggregatedEvents(TreeMap<Long, TreeMap<Long, OpenSearchDocument>> eventsMap) {
        final ArrayList<NeptuneEventAggregated> events = new ArrayList<>();

        assertSequence(new ArrayList<>(eventsMap.keySet()));

        for (Map.Entry<Long, TreeMap<Long, OpenSearchDocument>> commitSet : eventsMap.entrySet()) {

            assertSequence(new ArrayList<>(commitSet.getValue().keySet()));

            // We do this as a sub list to keep commits grouped and restricted
            final LinkedHashMap<String, NeptuneEventAggregated> commitEvents = new LinkedHashMap<>();

            for (Map.Entry<Long, OpenSearchDocument> opSet : commitSet.getValue().entrySet()) {
                OpenSearchDocument op = opSet.getValue();

                NeptuneEventAggregated event = commitEvents.get(op.getEntityId());
                if (event == null) {
                    event = new NeptuneEventAggregated(op.getEntityId());
                    commitEvents.put(op.getEntityId(), event);
                }

                event.consumeOpenSearchDocument(op);
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
    public final List<HashMap<String, Object>> predicatesToAdd = new ArrayList<>();
    public final List<HashMap<String, Object>> predicatesToDelete = new ArrayList<>();

    public NeptuneEventAggregated(String entityId) {
        this.entityId = entityId;
    }

    private ArrayList<HashMap<String, Object>> parsePredicates(Map<String, Set<OpenSearchDocumentPredicate>> predicates) {
        ArrayList<HashMap<String, Object>> parsedPredicates = new ArrayList<>();

        for (Map.Entry<String, Set<OpenSearchDocumentPredicate>> predicate : predicates.entrySet()) {
            HashMap<String, Object> map = new HashMap();
            map.put("key", predicate.getKey());
            map.put("value", predicate.getValue());
            parsedPredicates.add(map);
        }
        return parsedPredicates;
    }

    public void consumeOpenSearchDocument(OpenSearchDocument doc) {
        if (!doc.getEntityType().isEmpty()) {
            if (doc.getOp().equals(OP_TYPE_ADD)) {
                this.entityTypesToAdd.addAll(doc.getEntityType());

            } else if (doc.getOp().equals(OP_TYPE_DELETE)) {
                this.entityTypesToDelete.addAll(doc.getEntityType());
            }
        }

        if (!doc.getPredicates().isEmpty()) {
            if (doc.getOp().equals(OP_TYPE_ADD)) {
                this.predicatesToAdd.addAll(parsePredicates(doc.getPredicates()));

            } else if (doc.getOp().equals(OP_TYPE_DELETE)) {
                this.predicatesToDelete.addAll(parsePredicates(doc.getPredicates()));
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
