package io.rsocket.guide;

import java.util.HashMap;
import java.util.Map;

public class ChatData {
    public final Map<String, ChatChannel> channelByName = new HashMap<>();
}
