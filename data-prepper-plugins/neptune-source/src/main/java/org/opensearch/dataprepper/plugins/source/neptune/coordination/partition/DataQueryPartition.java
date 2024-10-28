package org.opensearch.dataprepper.plugins.source.neptune.coordination.partition;

import lombok.Getter;
import lombok.Setter;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.neptune.coordination.state.DataQueryProgressState;

import java.util.Optional;

@Setter
@Getter
public class DataQueryPartition extends EnhancedSourcePartition<DataQueryProgressState> {
    public static final String PARTITION_TYPE = "DATA_QUERY";

    private final String collection;
    private final String query;
    private final DataQueryProgressState state;

    public DataQueryPartition(final SourcePartitionStoreItem sourcePartitionStoreItem) {
        setSourcePartitionStoreItem(sourcePartitionStoreItem);
        collection = getCollection(sourcePartitionStoreItem.getSourcePartitionKey());
        query = sourcePartitionStoreItem.getSourcePartitionKey();
        this.state = convertStringToPartitionProgressState(DataQueryProgressState.class, sourcePartitionStoreItem.getPartitionProgressState());

    }

    private String getCollection(final String partitionKey) {
        String[] keySplits = partitionKey.split("\\|");
        return keySplits[0];
    }

    @Override
    public String getPartitionType() {
        return PARTITION_TYPE;
    }

    @Override
    public String getPartitionKey() {
        return query;
    }

    @Override
    public Optional<DataQueryProgressState> getProgressState() {
        if (state != null) {
            return Optional.of(state);
        }
        return Optional.empty();
    }
}
