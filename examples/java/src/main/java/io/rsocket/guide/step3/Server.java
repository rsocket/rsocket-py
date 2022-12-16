package io.rsocket.guide.step3;

import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.guide.step8.ChatChannel;
import io.rsocket.guide.step8.ChatData;
import io.rsocket.guide.step8.Session;
import io.rsocket.metadata.CompositeMetadata;
import io.rsocket.metadata.RoutingMetadata;
import io.rsocket.metadata.WellKnownMimeType;
import io.rsocket.pythontest.RoutingRSocket;
import io.rsocket.util.DefaultPayload;
import io.rsocket.util.EmptyPayload;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class Server implements SocketAcceptor {

    private final io.rsocket.guide.step8.ChatData chatData = new ChatData();

    @Override
    public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
        final var session = new Session();
        session.sessionId = UUID.randomUUID().toString();
        JSONParser jsonParser = new JSONParser();

        return Mono.just(new RSocket() {
            public Mono<Payload> requestResponse(Payload payload) {
                final var route = requireRoute(payload);

                switch (route) {
                    case "login":
                        session.username = payload.getDataUtf8();
                        return Mono.just(DefaultPayload.create(session.sessionId));
                    case "message":
                        try {
                            final var message = (JSONObject) jsonParser.parse(payload.getDataUtf8());
                            session.messages.add((String) message.get("content"));
                        } catch (ParseException e) {
                            throw new RuntimeException(e);
                        }

                        return Mono.just(EmptyPayload.INSTANCE);
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
                        final var next = session.messages.poll(20, TimeUnit.DAYS);
                        if (next != null) {
                            sink.next(DefaultPayload.create(next));
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
