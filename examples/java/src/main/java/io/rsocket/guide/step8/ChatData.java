package io.rsocket.guide.step8;

import java.util.HashMap;
import java.util.Map;

public class ChatData {
    public final Map<String, ChatChannel> channelByName = new HashMap<>();

    public final Map<String, Session> sessionById = new HashMap<>();
}