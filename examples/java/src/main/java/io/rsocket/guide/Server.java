package io.rsocket.guide;

import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.pythontest.RoutingRSocket;
import io.rsocket.pythontest.RoutingRSocketAdapter;
import io.rsocket.util.DefaultPayload;
import io.rsocket.util.EmptyPayload;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class Server implements SocketAcceptor {

    private final ChatData chatData = new ChatData();

    public void ensureChannel(String channelName) {
        chatData.channelByName.putIfAbsent(channelName, new ChatChannel());
    }

    public void join(String channel, String user) {
        chatData.channelByName.get(channel).users.add(user);
    }

    @Override
    public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
        final var session = new Session();
        session.sessionId = UUID.randomUUID().toString();
        JSONParser jsonParser = new JSONParser();
        return Mono.just(new RoutingRSocketAdapter(new RoutingRSocket() {

            public Mono<Void> fireAndForget(String route, Payload payload) {
                return Mono.defer(() -> {
                    switch (route) {
                        case "statistics":
                            final var str = payload.getDataUtf8();
                            System.out.println("Received :: " + str);
                            return Mono.empty();
                    }

                    return RoutingRSocket.super.fireAndForget(route, payload);
                });
            }

            public Mono<Payload> requestResponse(String route, Payload payload) {
                return Mono.defer(() -> {
                    switch (route) {
                        case "login":
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
                                final var message = (JSONObject) jsonParser.parse(payload.getDataUtf8());
                                session.messages.add((String) message.get("content"));
                            } catch (ParseException e) {
                                throw new RuntimeException(e);
                            }

                            return Mono.just(EmptyPayload.INSTANCE);
                    }

                    return RoutingRSocket.super.requestResponse(route, payload);
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
                        case "channel.users":
                            return Flux.fromIterable(chatData.channelByName.getOrDefault(payload.getDataUtf8(), new ChatChannel()).users)
                                    .map(DefaultPayload::create);
                    }

                    return RoutingRSocket.super.requestStream(route, payload);
                });
            }

            @Override
            public Flux<Payload> requestChannel(String route, Publisher<Payload> payloads) {
                return Flux.defer(() -> {
                    switch (route) {
                        case "statistics":
                            return Flux.from(payloads).count()
                                    .doOnNext(count -> System.out.println("Received :: " + count))
                                    .thenMany(Flux.range(0, 3)
                                            .map(index -> "Item on channel: " + index)
                                            .map(DefaultPayload::create));
                    }

                    return RoutingRSocket.super.requestChannel(route, payloads);
                });
            }
        }));
    }

    private void leave(String channel, String sessionId) {
        chatData.channelByName.get(channel).users.remove(sessionId);
    }
}
