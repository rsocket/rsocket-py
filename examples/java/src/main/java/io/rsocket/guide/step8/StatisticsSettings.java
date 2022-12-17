package io.rsocket.guide.step8;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Set;

public class StatisticsSettings {
    public Set<String> ids = Set.of("users");

    @JsonProperty("period_seconds")
    public Integer periodSeconds = 5;

    public StatisticsSettings() {
    }

    public StatisticsSettings(Set<String> ids, Integer periodSeconds) {
        this.ids = ids;
        this.periodSeconds = periodSeconds;
    }
}
