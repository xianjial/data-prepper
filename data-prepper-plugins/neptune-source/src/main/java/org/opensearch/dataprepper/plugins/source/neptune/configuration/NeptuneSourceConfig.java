package org.opensearch.dataprepper.plugins.source.neptune.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;

import java.time.Duration;

@Getter
@Setter
public class NeptuneSourceConfig {
    private static final int DEFAULT_PORT = 8182;
    private static final Duration DEFAULT_ACKNOWLEDGEMENT_SET_TIMEOUT = Duration.ofHours(2);

    @JsonProperty("host")
    private @NotNull String host;
    @JsonProperty("port")
    private int port = DEFAULT_PORT;
    @JsonProperty("region")
    private String region;
    @JsonProperty("iamAuth")
    private Boolean iamAuth = false;

    @JsonProperty("trust_store_file_path")
    private String trustStoreFilePath;
    @JsonProperty("trust_store_password")
    private String trustStorePassword;

    @JsonProperty("s3_bucket")
    private String s3Bucket;
    @JsonProperty("s3_prefix")
    private String s3Prefix;
    @JsonProperty("s3_region")
    private String s3Region;

    @JsonProperty("acknowledgments")
    private Boolean acknowledgments = false;
    @JsonProperty
    private Duration partitionAcknowledgmentTimeout;

    @JsonProperty("aws")
    @NotNull
    @Valid
    private AwsConfig awsConfig;

    @JsonProperty("export")
    private boolean export;
    @JsonProperty("stream")
    private boolean stream;
    @JsonProperty("streamType")
    private String streamType;

    public NeptuneSourceConfig() {
        this.partitionAcknowledgmentTimeout = DEFAULT_ACKNOWLEDGEMENT_SET_TIMEOUT;
    }
}
