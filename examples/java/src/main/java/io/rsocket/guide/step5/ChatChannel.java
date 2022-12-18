package io.rsocket.guide.step5;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

public class ChatChannel {
    public String name;

    final public BlockingQueue<Message> messages = new LinkedBlockingQueue<>();

    final public AtomicReference<Thread> messageRouter = new AtomicReference<>();

    final public Set<String> users = new HashSet<>();
}
