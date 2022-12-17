package io.rsocket.guide.step8;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ServerStatistic {
    @JsonProperty("user_count")
    public Integer userCount;

    @JsonProperty("channel_count")
    public Integer channelCount;

    public ServerStatistic() {

    }

    public ServerStatistic(Integer userCount, Integer channelCount) {
        this.userCount = userCount;
        this.channelCount = channelCount;
    }
}
