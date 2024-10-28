package org.opensearch.dataprepper.plugins.source.neptune.converter;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Getter;
import org.opensearch.dataprepper.model.event.Event;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;


@Builder
@Getter
public class OpenSearchDocument {

    public static final String VERTEX_ID_PREFIX = "v://";
    public static final String EDGE_ID_PREFIX = "e://";

    // The operation for the entity (ADD, UPDATE etc.)
    @JsonProperty("op")
    private String op;

    // The commit number to the database
    @JsonProperty("commit_num")
    private Long commitNum;

    // The operation number of the commit
    @JsonProperty("op_num")
    private Long opNum;

    // Reference to Neptune entity corresponding to document. For Gremlin, it will be Vertex Id for Vertex document &
    // Edge ID for Edge Document. For Sparql, it will be RDF subject URI
    @JsonProperty("entity_id")
    private String entityId;

    // Store the Neptune entity type(s). Vertex/Edge label for gremlin. rdf:type for Sparql
    // Note that Gremlin Vertexes and Sparql can have multiple types.
    @JsonProperty("entity_type")
    private Set<String> entityType;

    // Classify Open Search document. It could be one of vertex / edge / rdf-resource
    @JsonProperty("document_type")
    private String documentType;

    // Nested Field for storing predicates corresponding to Graph vertex / Edge
    @JsonProperty("predicates")
    private Map<String, Set<OpenSearchDocumentPredicate>> predicates;


    // merges two opensearch documents for the same entityId
    // for ex
    // d1
    // <s1> rdf:type Person.
    // <s1> <knows> "Mohamed".

    // d2
    // <s1> rdf:type Human.
    // <s1> <knows> "Andreas".
    // <s1> <plays> "Football".

    // result
    // {entity_type: [Person, Human], predicates: {"knows": [Mohamed, Andreas], "plays": [football]}}

    public void merge(final OpenSearchDocument other) {
        // must be same entity
        if (!this.entityId.equalsIgnoreCase(other.entityId)) {
            return;
        }

        // merge labels
        this.getEntityType().addAll(other.getEntityType());

        // merge predicates
        for (final Map.Entry<String, Set<OpenSearchDocumentPredicate>> otherPredicates : other.getPredicates().entrySet()) {
            // if curr doc doesn't have the predicate, just add it
            if (!this.predicates.containsKey(otherPredicates.getKey())) {
                this.predicates.put(otherPredicates.getKey(), otherPredicates.getValue());
                continue;
            }

            // predicate exists, need to merge them
            final Set<OpenSearchDocumentPredicate> currPredicateValue = this.getPredicates().get(otherPredicates.getKey());
            currPredicateValue.addAll(otherPredicates.getValue());
        }
    }

    public static OpenSearchDocument fromEvent(Event event) {
        final String op = event.get("op", String.class);
        final Long commitNum = event.get("commit_num", Long.class);
        final Long opNum = event.get("op_num", Long.class);
        final String entityId = event.get("entity_id", String.class);
        final Set<String> entityType = event.get("entity_type", Set.class);
        final String documentType = event.get("document_type", String.class);

        final HashMap<String, Object> predicatesRaw = event.get("predicates", HashMap.class);
        final Map<String, Set<OpenSearchDocumentPredicate>> predicates = new HashMap<>();

        for (Map.Entry<String, Object> entry : predicatesRaw.entrySet()) {
            predicates.putIfAbsent(entry.getKey(), new HashSet<>());
            predicates.get(entry.getKey()).add(OpenSearchDocumentPredicate
                    .builder()
                    .value((String) predicatesRaw.get("value"))
                    .graph((String) predicatesRaw.get("graph"))
                    .language((String) predicatesRaw.get("language"))
                    .build());
        }

        return OpenSearchDocument.builder()
                .op(op)
                .commitNum(commitNum)
                .opNum(opNum)
                .entityId(entityId)
                .entityType(entityType)
                .documentType(documentType)
                .predicates(predicates)
                .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OpenSearchDocument that = (OpenSearchDocument) o;
        if (!(Objects.equals(op, that.op) && Objects.equals(commitNum, that.commitNum) && Objects.equals(opNum, that.opNum) && Objects.equals(entityId, that.entityId) && Objects.equals(entityType, that.entityType) && Objects.equals(documentType, that.documentType))) {
            return false;
        }

        for (Map.Entry<String, Set<OpenSearchDocumentPredicate>> expectedPredicates : this.getPredicates().entrySet()) {
            Set<OpenSearchDocumentPredicate> actualPredicates = that.getPredicates().get(expectedPredicates.getKey());

            if (actualPredicates == null) return false;

            List<OpenSearchDocumentPredicate> expectedPredicateList = new ArrayList<>(expectedPredicates.getValue());
            List<OpenSearchDocumentPredicate> actualPredicateList = new ArrayList<>(actualPredicates);

            if (expectedPredicateList.size() != actualPredicateList.size()) return false;

            for (int i = 0; i < expectedPredicateList.size(); i++) {
                OpenSearchDocumentPredicate expectedPredicate = expectedPredicateList.get(i);
                OpenSearchDocumentPredicate actualPredicate = actualPredicateList.get(i);

                if (!(expectedPredicate.equals(actualPredicate))) return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(op, commitNum, opNum, entityId, entityType, documentType, predicates);
    }
}
