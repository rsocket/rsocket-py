package io.rsocket.guide;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ChatChannel {
    public String name;

    final public BlockingQueue<String> messages = new LinkedBlockingQueue<>();

    final public Set<String> users = new HashSet<>();
}
