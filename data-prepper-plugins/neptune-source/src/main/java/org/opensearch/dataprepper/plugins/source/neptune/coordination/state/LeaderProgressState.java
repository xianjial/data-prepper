package org.opensearch.dataprepper.plugins.source.neptune.coordination.state;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class LeaderProgressState {

    @JsonProperty("initialized")
    private boolean initialized = false;
}
