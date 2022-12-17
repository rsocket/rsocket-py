package io.rsocket.guide.step8;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ClientStatistics {
    @JsonProperty("memory_usage")
    public Long memoryUsage;

    public ClientStatistics() {
    }

    public ClientStatistics(Long memoryUsage) {
        this.memoryUsage = memoryUsage;
    }
}
