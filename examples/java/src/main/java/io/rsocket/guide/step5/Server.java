package io.rsocket.guide.step5;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.guide.step8.MimeTypes;
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

    public void ensureChannel(String channelName) {
        if (!chatData.channelByName.containsKey(channelName)) {
            ChatChannel chatChannel = new ChatChannel();
            chatChannel.name = channelName;
            chatData.channelByName.put(channelName, chatChannel);
            final var thread = new Thread(() -> channelMessageRouter(channelName));
            thread.start();
            chatChannel.messageRouter.set(thread);
        }
    }

    public void channelMessageRouter(String channelName) {
        final var channel = chatData.channelByName.get(channelName);
        while (true) {
            try {
                final var message = channel.messages.poll(20, TimeUnit.DAYS);
                if (message != null) {
                    for (String user : channel.users) {
                        findUserByName(user).doOnNext(session -> {
                            try {
                                session.messages.put(message);
                            } catch (InterruptedException exception) {
                                throw new RuntimeException(exception);
                            }
                        }).block();
                    }
                }
            } catch (Exception exception) {
                break;
            }
        }
    }

    public void join(String channel, String user) {
        chatData.channelByName.get(channel).users.add(user);
    }

    private void leave(String channel, String sessionId) {
        chatData.channelByName.get(channel).users.remove(sessionId);
    }

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
                    case "channel.join":
                        final var channelJoin = payload.getDataUtf8();
                        ensureChannel(channelJoin);
                        join(channelJoin, session.sessionId);
                        return Mono.just(EmptyPayload.INSTANCE);
                    case "channel.leave":
                        leave(payload.getDataUtf8(), session.sessionId);
                        return Mono.just(EmptyPayload.INSTANCE);
                    case "message":
                        final var message = fromJson(payload.getDataUtf8(), Message.class);
                        final var targetMessage = new Message(session.username, message.content, message.channel);

                        if (message.channel != null) {
                            chatData.channelByName.get(message.channel).messages.add(targetMessage);
                        } else {

                            return findUserByName(message.user)
                                    .doOnNext(targetSession -> targetSession.messages.add(targetMessage))
                                    .thenReturn(EmptyPayload.INSTANCE);
                        }
                    case "file.upload":
                        chatData.filesByName.put(requireFilename(payload), payload.sliceData());
                        return Mono.just(EmptyPayload.INSTANCE);
                    case "file.download":
                        return Mono.just(DefaultPayload.create(chatData.filesByName.get(requireFilename(payload))));
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
                            sink.next(DefaultPayload.create(toJson(message)));
                        }
                    } catch (InterruptedException exception) {
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
                        case "files":
                            return Flux.fromIterable(chatData.filesByName.keySet()).map(DefaultPayload::create);
                    }

                    throw new IllegalStateException();
                });
            }
        });
    }

    private String requireFilename(Payload payload) {
        final var metadata = payload.sliceMetadata();
        final CompositeMetadata compositeMetadata = new CompositeMetadata(metadata, false);

        for (CompositeMetadata.Entry metadatum : compositeMetadata) {
            if (Objects.requireNonNull(metadatum.getMimeType()).equals(MimeTypes.fileMimeType)) {
                return metadatum.getContent().toString();
            }
        }

        throw new IllegalStateException();
    }

    private <T> String toJson(T serverStatistic) {
        try {
            return objectMapper.writeValueAsString(serverStatistic);
        } catch (JsonProcessingException exception) {
            return "{}";
        }
    }

    private <T> T fromJson(String dataUtf8, Class<T> cls) {
        try {
            return objectMapper.readValue(dataUtf8, cls);

        } catch (Exception exception) {
            throw new RuntimeException(exception);
        }
    }
}
