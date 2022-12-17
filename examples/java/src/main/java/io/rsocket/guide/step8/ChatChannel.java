package io.rsocket.guide.step8;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ChatChannel {
    public String name;

    final public BlockingQueue<Message> messages = new LinkedBlockingQueue<>();

    final public Set<String> users = new HashSet<>();
}
