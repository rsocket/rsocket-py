package io.rsocket.guide.step3;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.metadata.CompositeMetadata;
import io.rsocket.metadata.RoutingMetadata;
import io.rsocket.metadata.WellKnownMimeType;
import io.rsocket.util.DefaultPayload;
import io.rsocket.util.EmptyPayload;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class Server implements SocketAcceptor {

    private final ChatData chatData = new ChatData();

    final ObjectMapper objectMapper = new ObjectMapper();

    public Mono<Session> findUserByName(final String username) {
        return Flux.fromIterable(chatData.sessionById.entrySet())
                .filter(e -> e.getValue().username.equals(username))
                .map(Map.Entry::getValue)
                .single();
    }

    @Override
    public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
        final var session = new Session();
        session.sessionId = UUID.randomUUID().toString();
        chatData.sessionById.put(session.sessionId, session);

        return Mono.just(new RSocket() {
            public Mono<Payload> requestResponse(Payload payload) {
                final var route = requireRoute(payload);

                switch (route) {
                    case "login":
                        session.username = payload.getDataUtf8();
                        return Mono.just(DefaultPayload.create(session.sessionId));
                    case "message":
                        try {
                            final var message = objectMapper.readValue(payload.getDataUtf8(), Message.class);
                            final var targetMessage = new Message(session.username, message.content);
                            return findUserByName(message.user)
                                    .doOnNext(targetSession -> targetSession.messages.add(targetMessage))
                                    .thenReturn(EmptyPayload.INSTANCE);
                        } catch (Exception exception) {
                            throw new RuntimeException(exception);
                        }
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

            public void messageSupplier(FluxSink<Payload> sink) {
                while (true) {
                    try {
                        final var message = session.messages.poll(20, TimeUnit.DAYS);
                        if (message != null) {
                            sink.next(DefaultPayload.create(objectMapper.writeValueAsString(message)));
                        }
                    } catch (Exception exception) {
                        break;
                    }
                }
            }

            public Flux<Payload> requestStream(String route, Payload payload) {
                return Flux.defer(() -> {
                    switch (route) {
                        case "messages.incoming":
                            final var threadContainer = new AtomicReference<Thread>();
                            return Flux.create(sink -> sink.onRequest(n -> {
                                        if (threadContainer.get() == null) {
                                            final var thread = new Thread(() -> messageSupplier(sink));
                                            thread.start();
                                            threadContainer.set(thread);
                                        }
                                    })
                                    .onCancel(() -> threadContainer.get().interrupt())
                                    .onDispose(() -> threadContainer.get().interrupt()));
                    }

                    throw new IllegalStateException();
                });
            }
        });
    }
}
