package org.opensearch.dataprepper.plugins.source.neptune.stream;

import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.neptune.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.source.neptune.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.neptune.coordination.state.StreamProgressState;
import org.opensearch.dataprepper.plugins.source.neptune.model.StreamLoadStatus;
import org.opensearch.dataprepper.plugins.source.neptune.s3partition.S3FolderPartitionCoordinator;
import org.opensearch.dataprepper.plugins.source.neptune.stream.model.StreamCheckpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;

import static org.opensearch.dataprepper.model.source.s3.S3ScanEnvironmentVariables.STOP_S3_SCAN_PROCESSING_PROPERTY;

/**
 * A helper class to handle the data query partition status and the progress state
 * It will use coordinator APIs under the hood.
 */
public class DataStreamPartitionCheckpoint extends S3FolderPartitionCoordinator {
    private static final Logger LOG = LoggerFactory.getLogger(DataStreamPartitionCheckpoint.class);

    public static final String STREAM_PREFIX = "STREAM-";
    static final Duration CHECKPOINT_OWNERSHIP_TIMEOUT_INCREASE = Duration.ofMinutes(5);

    private final EnhancedSourceCoordinator enhancedSourceCoordinator;

    private final StreamPartition streamPartition;

    public DataStreamPartitionCheckpoint(final EnhancedSourceCoordinator enhancedSourceCoordinator,
                                         final StreamPartition streamPartition) {
        super(enhancedSourceCoordinator);
        this.enhancedSourceCoordinator = enhancedSourceCoordinator;
        this.streamPartition = streamPartition;
    }

    private void setProgressState(final StreamCheckpoint progress) {
        //Always has a state.
        Optional<StreamProgressState> progressState = streamPartition.getProgressState();
        progressState.get().updateFromCheckpoint(progress);
    }

    /**
     * This method is to do a checkpoint with latest resume token processed.
     * Note that this should be called on a regular basis even there are no changes to resume token
     * As the checkpoint will also extend the timeout for the lease
     *
     */
    public void checkpoint(final StreamCheckpoint checkpointProgress) {
        LOG.debug("Checkpoint stream partition with record number {}", checkpointProgress.getRecordCount());
        setProgressState(checkpointProgress);
        enhancedSourceCoordinator.saveProgressStateForPartition(streamPartition, CHECKPOINT_OWNERSHIP_TIMEOUT_INCREASE);
    }


    public void extendLease() {
        LOG.debug("Extending lease of stream partition");
        enhancedSourceCoordinator.saveProgressStateForPartition(streamPartition, CHECKPOINT_OWNERSHIP_TIMEOUT_INCREASE);
    }

    /**
     * This method is to reset checkpoint when change stream is invalid. The current thread will give up partition and new thread
     * will take ownership of partition. If change stream is valid then new thread proceeds with processing change stream else the
     * process repeats.
     */
    public void resetCheckpoint() {
        LOG.debug("Resetting checkpoint stream partition");
        setProgressState(StreamCheckpoint.emptyProgress());
        enhancedSourceCoordinator.giveUpPartition(streamPartition);
        System.clearProperty(STOP_S3_SCAN_PROCESSING_PROPERTY);
    }

    public Optional<StreamLoadStatus> getGlobalStreamLoadStatus() {
        final Optional<EnhancedSourcePartition> partition = enhancedSourceCoordinator.getPartition(STREAM_PREFIX + streamPartition.getPartitionKey());
        if (partition.isPresent()) {
            final GlobalState globalState = (GlobalState) partition.get();
            return Optional.of(StreamLoadStatus.fromMap(globalState.getProgressState().get()));
        } else {
            return Optional.empty();
        }
    }

    public void updateStreamPartitionForAcknowledgmentWait(final Duration acknowledgmentSetTimeout) {
        enhancedSourceCoordinator.saveProgressStateForPartition(streamPartition, acknowledgmentSetTimeout);
    }

    public void giveUpPartition() {
        enhancedSourceCoordinator.giveUpPartition(streamPartition);
        System.clearProperty(STOP_S3_SCAN_PROCESSING_PROPERTY);
    }
}
