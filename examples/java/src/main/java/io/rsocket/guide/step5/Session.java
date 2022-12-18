package io.rsocket.guide.step5;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Session {
    final public BlockingQueue<Message> messages = new LinkedBlockingQueue<>();

    public String username;

    public String sessionId;
}
