package io.rsocket.guide.step5;

import io.rsocket.core.RSocketServer;
import io.rsocket.transport.netty.server.TcpServerTransport;

public class ServerApplication {

    public static void main(String[] args) {
        final var transport = TcpServerTransport.create(6565);

        RSocketServer.create()
                .acceptor(new Server())
                .bind(transport)
                .block()
                .onClose()
                .block();
    }
}
