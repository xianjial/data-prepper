package org.opensearch.dataprepper.plugins.source.neptune.converter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;

public class OpenSearchDocumentTest {
    private Event neptuneStreamEvent;
    private OpenSearchDocument expectedDoc;

    @BeforeEach
    void setup() {
        expectedDoc = OpenSearchDocument
                .builder()
                .op("ADD")
                .commitNum(1L)
                .opNum(2L)
                .entityId("v://22c94d6b-f537-62f6-6407-f85fd4b59629")
                .documentType("vertex")
                .entityType(Set.of("Person"))
                .predicates(Map.of("name", Set.of(OpenSearchDocumentPredicate.builder().value("dan").graph("g1").language("en").build())))
                .build();
        neptuneStreamEvent = JacksonEvent.builder()
                .withEventType("event")
                .withData(expectedDoc)
                .build();
    }

    @Test
    void fromEvent() {
        final OpenSearchDocument actualDoc = OpenSearchDocument.fromEvent(neptuneStreamEvent);

        assertEquals(expectedDoc.getOp(), actualDoc.getOp());
        assertEquals(expectedDoc.getCommitNum(), actualDoc.getCommitNum());
        assertEquals(expectedDoc.getOpNum(), actualDoc.getOpNum());
        assertEquals(expectedDoc.getEntityId(), actualDoc.getEntityId());
        assertEquals(expectedDoc.getDocumentType(), actualDoc.getDocumentType());
        assertEquals(expectedDoc.getEntityType(), actualDoc.getEntityType());

        for (Map.Entry<String, Set<OpenSearchDocumentPredicate>> expectedPredicates : expectedDoc.getPredicates().entrySet()) {
            Set<OpenSearchDocumentPredicate> actualPredicates = actualDoc.getPredicates().get(expectedPredicates.getKey());

            assertNotNull(actualDoc.getPredicates().get(expectedPredicates.getKey()), "Predicate with key " + expectedPredicates.getKey() + " not found");

            List<OpenSearchDocumentPredicate> expectedPredicateList = new ArrayList<>(expectedPredicates.getValue());
            List<OpenSearchDocumentPredicate> actualPredicateList = new ArrayList<>(actualPredicates);

            assertEquals(expectedPredicateList.size(), actualPredicateList.size());

            for (int i = 0; i < expectedPredicateList.size() - 1; i++) {
                OpenSearchDocumentPredicate expectedPredicate = expectedPredicateList.get(i);
                OpenSearchDocumentPredicate actualPredicate = actualPredicateList.get(i);

                assertEquals(expectedPredicate.getValue(), actualPredicate.getValue());
                assertEquals(expectedPredicate.getGraph(), actualPredicate.getGraph());
                assertEquals(expectedPredicate.getLanguage(), actualPredicate.getLanguage());
            }
        }
    }
}
