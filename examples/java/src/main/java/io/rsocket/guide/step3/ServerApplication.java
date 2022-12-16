package io.rsocket.guide.step3;

import io.rsocket.core.RSocketServer;
import io.rsocket.guide.step8.Server;
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
