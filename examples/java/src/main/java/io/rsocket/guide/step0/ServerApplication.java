package io.rsocket.guide.step0;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketServer;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import reactor.core.publisher.Mono;

public class ServerApplication {

    public static void main(String[] args) {
        final var transport = TcpServerTransport.create(6565);

        final SocketAcceptor socketAcceptor = (setup, sendingSocket) -> Mono.just(new RSocket() {
            public Mono<Payload> requestResponse(Payload payload) {
                return Mono.just(DefaultPayload.create("Welcome to chat, " + payload.getDataUtf8()));
            }
        });

        RSocketServer.create()
                .acceptor(socketAcceptor)
                .bind(transport)
                .block()
                .onClose()
                .block();
    }
}
