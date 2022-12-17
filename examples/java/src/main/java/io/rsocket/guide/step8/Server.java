package io.rsocket.guide.step8;

import com.fasterxml.jackson.core.JsonProcessingException;
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
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class Server implements SocketAcceptor {

    private final ChatData chatData = new ChatData();

    public void ensureChannel(String channelName) {
        if (!chatData.channelByName.containsKey(channelName)) {
            ChatChannel chatChannel = new ChatChannel();
            chatChannel.name = channelName;
            chatData.channelByName.put(channelName, chatChannel);
        }
    }

    public void join(String channel, String user) {
        chatData.channelByName.get(channel).users.add(user);
    }

    @Override
    public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
        final var session = new Session();
        session.sessionId = UUID.randomUUID().toString();
        final var objectMapper = new ObjectMapper();
        return Mono.just(new RSocket() {

            public Mono<Void> fireAndForget(Payload payload) {
                final var route = requireRoute(payload);

                return Mono.defer(() -> {
                    switch (route) {
                        case "statistics":
                            final var str = payload.getDataUtf8();
                            System.out.println("Received :: " + str);
                            return Mono.empty();
                    }

                    throw new IllegalStateException();
                });
            }

            public Mono<Payload> requestResponse(Payload payload) {
                final var route = requireRoute(payload);

                return Mono.defer(() -> {
                    switch (route) {
                        case "login":
                            session.username = payload.getDataUtf8();
                            return Mono.just(DefaultPayload.create(session.sessionId));
                        case "channel.join":
                            final var channelJoin = payload.getDataUtf8();
                            ensureChannel(channelJoin);
                            join(channelJoin, session.sessionId);
                            return Mono.just(EmptyPayload.INSTANCE);
                        case "channel.leave":
                            leave(payload.getDataUtf8(), session.sessionId);
                            return Mono.just(EmptyPayload.INSTANCE);
                        case "message":
                            try {
                                final var message = objectMapper.readValue(payload.getDataUtf8(), Message.class);
                                session.messages.add(message.content);
                            } catch (Exception exception) {
                                throw new RuntimeException(exception);
                            }

                            return Mono.just(EmptyPayload.INSTANCE);
                    }

                    throw new IllegalStateException();
                });
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

            public Flux<Payload> requestStream(Payload payload) {
                final var route = requireRoute(payload);

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
                        case "channel.users":
                            return Flux.fromIterable(chatData.channelByName.getOrDefault(payload.getDataUtf8(), new ChatChannel()).users)
                                    .map(DefaultPayload::create);
                    }

                    throw new IllegalStateException();
                });
            }

            public void consumeStatisticSettings(StatisticsSettings settings) {
                session.statisticsSettings = settings;
            }

            @Override
            public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                return Flux.from(payloads).switchOnFirst((firstSignal, others) -> {
                    Payload firstPayload = firstSignal.get();
                    if (firstPayload != null) {
                        final var settings = parseStatistics(firstPayload);
                        final var route = requireRoute(firstPayload);
                        switch (route) {
                            case "statistics":
                                Flux.from(others.map(this::parseStatistics).skip(1).startWith(settings))
                                        .subscribe(this::consumeStatisticSettings);

                                return Flux.interval(Duration.ofSeconds(1)) // todo: Modifiable delay
                                        .map(index -> new Statistic(chatData.sessionById.size(), chatData.channelByName.size()))
                                        .map(statistic -> {
                                            try {
                                                return objectMapper.writeValueAsString(statistic);
                                            } catch (JsonProcessingException exception) {
                                                return "{}";
                                            }
                                        })
                                        .map(DefaultPayload::create);
                        }
                    }
                    throw new IllegalStateException();
                });
            }

            private StatisticsSettings parseStatistics(Payload e) {
                try {
                    return objectMapper.readValue(e.getDataUtf8(), StatisticsSettings.class);
                } catch (JsonProcessingException exception) {
                    return new StatisticsSettings();
                }
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

    private void leave(String channel, String sessionId) {
        chatData.channelByName.get(channel).users.remove(sessionId);
    }
}
