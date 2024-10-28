/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.neptune.coordination.state;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class DataQueryProgressState {
    @JsonProperty("executedQueries")
    private long executedQueries;

    @JsonProperty("loadedRecords")
    private long loadedRecords;

    @JsonProperty("exportStartTime")
    private long startTime;

}
