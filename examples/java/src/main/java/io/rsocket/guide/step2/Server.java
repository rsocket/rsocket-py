package io.rsocket.guide.step2;

import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.guide.step8.Session;
import io.rsocket.metadata.CompositeMetadata;
import io.rsocket.metadata.RoutingMetadata;
import io.rsocket.metadata.WellKnownMimeType;
import io.rsocket.util.DefaultPayload;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.UUID;

public class Server implements SocketAcceptor {

    @Override
    public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
        final var session = new Session();
        session.sessionId = UUID.randomUUID().toString();

        return Mono.just(new RSocket() {
            public Mono<Payload> requestResponse(Payload payload) {
                final var route = requireRoute(payload);

                switch (route) {
                    case "login":
                        session.username = payload.getDataUtf8();
                        return Mono.just(DefaultPayload.create(session.sessionId));
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
    }
}
