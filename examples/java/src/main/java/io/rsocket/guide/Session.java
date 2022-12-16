package io.rsocket.guide;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Session {
    final public BlockingQueue<String> messages = new LinkedBlockingQueue<>();

    public String username;

    public String sessionId;
}
