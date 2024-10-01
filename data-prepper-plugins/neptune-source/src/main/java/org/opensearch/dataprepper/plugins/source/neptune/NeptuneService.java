package org.opensearch.dataprepper.plugins.source.neptune;

import com.amazonaws.neptune.auth.NeptuneSigV4SignerException;
import org.opensearch.dataprepper.common.concurrent.BackgroundThreadFactory;
import org.opensearch.dataprepper.plugins.source.neptune.leader.LeaderScheduler;
import org.opensearch.dataprepper.plugins.source.neptune.stream.StreamScheduler;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.neptune.configuration.NeptuneSourceConfig;
import org.opensearch.dataprepper.plugins.source.neptune.s3partition.S3PartitionCreatorScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This service class
 * - reading stream data from Neptune Stream Http Endpoints
 * - export (initial load) - to be implemented later
 */
public class NeptuneService {
    private static final Logger LOG = LoggerFactory.getLogger(NeptuneService.class);

    private static final String S3_PATH_DELIMITER = "/";

    private final EnhancedSourceCoordinator sourceCoordinator;
    private final PluginMetrics pluginMetrics;
    private final NeptuneSourceConfig sourceConfig;
    private final AcknowledgementSetManager acknowledgementSetManager;

    private ExecutorService executor;

    public NeptuneService(final EnhancedSourceCoordinator sourceCoordinator,
                          final NeptuneSourceConfig sourceConfig,
                          final PluginMetrics pluginMetrics,
                          final AcknowledgementSetManager acknowledgementSetManager) throws NeptuneSigV4SignerException {
        this.sourceCoordinator = sourceCoordinator;
        this.pluginMetrics = pluginMetrics;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.sourceConfig = sourceConfig;
    }

    /**
     * This service start 3 long-running threads (scheduler)
     * - Leader Scheduler
     * - Stream Scheduler
     * -
     * Each thread is responsible for one type of job.
     * The data will be guaranteed to be sent to {@link Buffer} in order.
     *
     * @param buffer Data Prepper Buffer
     */
    public void start(Buffer<Record<Event>> buffer) {
        LOG.info("Start running Neptune service");

        final String s3PathPrefix = getS3PathPrefix();
        final LeaderScheduler leaderScheduler = new LeaderScheduler(sourceCoordinator, sourceConfig, s3PathPrefix);
        final S3PartitionCreatorScheduler s3PartitionCreatorScheduler = new S3PartitionCreatorScheduler(sourceCoordinator);
        final StreamScheduler streamScheduler = new StreamScheduler(sourceCoordinator, buffer, acknowledgementSetManager, sourceConfig, s3PathPrefix, pluginMetrics);

        executor = Executors.newFixedThreadPool(3, BackgroundThreadFactory.defaultExecutorThreadFactory("neptune-source"));
        executor.submit(leaderScheduler);
        executor.submit(s3PartitionCreatorScheduler);
        executor.submit(streamScheduler);
    }

    private String getS3PathPrefix() {
        final String s3UserPathPrefix;
        if (sourceConfig.getS3Prefix() != null && !sourceConfig.getS3Prefix().isBlank()) {
            s3UserPathPrefix = sourceConfig.getS3Prefix() + S3_PATH_DELIMITER;
        } else {
            s3UserPathPrefix = "";
        }

        final String s3PathPrefix;
        final Instant now = Instant.now();
        if (sourceCoordinator.getPartitionPrefix() != null ) {
            s3PathPrefix = s3UserPathPrefix + sourceCoordinator.getPartitionPrefix() + S3_PATH_DELIMITER + now.toEpochMilli() + S3_PATH_DELIMITER;
        } else {
            s3PathPrefix = s3UserPathPrefix + now.toEpochMilli() + S3_PATH_DELIMITER;
        }
        return s3PathPrefix;
    }

    /**
     * Interrupt the running of schedulers.
     * Each scheduler must implement logic for gracefully shutdown.
     */
    public void shutdown() {
        if (executor != null) {
            LOG.info("shutdown Neptune Service scheduler and worker");
            executor.shutdownNow();
        }
    }
}
