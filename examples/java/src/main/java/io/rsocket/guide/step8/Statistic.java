package io.rsocket.guide.step8;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Statistic {
    @JsonProperty("user_count")
    public Integer userCount;

    @JsonProperty("channel_count")
    public Integer channelCount;

    public Statistic() {

    }

    public Statistic(Integer userCount, Integer channelCount) {
        this.userCount = userCount;
        this.channelCount = channelCount;
    }
}
