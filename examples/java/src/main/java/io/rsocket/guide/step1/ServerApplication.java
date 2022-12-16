package io.rsocket.guide.step1;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketServer;
import io.rsocket.metadata.CompositeMetadata;
import io.rsocket.metadata.RoutingMetadata;
import io.rsocket.metadata.WellKnownMimeType;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import reactor.core.publisher.Mono;

import java.util.Objects;

public class ServerApplication {

    public static void main(String[] args) {
        final var transport = TcpServerTransport.create(6565);

        final SocketAcceptor socketAcceptor = (setup, sendingSocket) -> Mono.just(new RSocket() {
            public Mono<Payload> requestResponse(Payload payload) {
                final var route = requireRoute(payload);

                switch (route) {
                    case "login":
                        return Mono.just(DefaultPayload.create("Welcome to chat, " + payload.getDataUtf8()));
                }

                throw new RuntimeException("Unknown requestResponse route " + route);
            }

            private String requireRoute(Payload payload) {
                final var metadata = payload.sliceMetadata();
                final CompositeMetadata compositeMetadata = new CompositeMetadata(metadata, false);

                for (CompositeMetadata.Entry metadatum : compositeMetadata) {
                    if (Objects.requireNonNull(metadatum.getMimeType())
                            .equals(WellKnownMimeType.MESSAGE_RSOCKET_ROUTING.getString())) {
                        return new RoutingMetadata(metadatum.getContent()).iterator().next();
                    }
                }

                throw new IllegalStateException();
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
