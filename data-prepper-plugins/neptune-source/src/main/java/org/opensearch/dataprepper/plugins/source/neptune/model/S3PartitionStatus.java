package org.opensearch.dataprepper.plugins.source.neptune.model;

import lombok.Getter;

import java.util.List;
import java.util.Map;

@Getter
public class S3PartitionStatus {

    private static final String PARTITIONS = "partitions";

    private final List<String> partitions;

    public S3PartitionStatus(final List<String> partitions) {
        this.partitions = partitions;
    }

    public Map<String, Object> toMap() {
        return Map.of(
                PARTITIONS, partitions
        );
    }

    public static S3PartitionStatus fromMap(Map<String, Object> map) {
        return new S3PartitionStatus(
                ((List<String>) map.get(PARTITIONS))
        );
    }
}
