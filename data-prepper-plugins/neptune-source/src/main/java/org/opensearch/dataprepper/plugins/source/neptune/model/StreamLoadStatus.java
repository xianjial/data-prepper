package org.opensearch.dataprepper.plugins.source.neptune.model;

import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Setter
@Getter
public class StreamLoadStatus {

    private static final String EXPORT_END_TIMESTAMP = "exportEndTimestamp";

    private long exportEndTimestamp;

    public StreamLoadStatus(long exportEndTimestamp) {
        this.exportEndTimestamp = exportEndTimestamp;
    }

    public Map<String, Object> toMap() {
        return Map.of(
                EXPORT_END_TIMESTAMP, exportEndTimestamp
        );
    }

    public static StreamLoadStatus fromMap(Map<String, Object> map) {
        return new StreamLoadStatus(
                ((Number) map.get(EXPORT_END_TIMESTAMP)).longValue()
        );
    }
}
