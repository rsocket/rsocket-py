package io.rsocket.guide;

import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.pythontest.Fixtures;
import io.rsocket.pythontest.RoutingRSocket;
import io.rsocket.pythontest.RoutingRSocketAdapter;
import io.rsocket.util.DefaultPayload;
import io.rsocket.util.EmptyPayload;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.channels.SeekableByteChannel;
import java.util.concurrent.TimeUnit;

public class Server implements SocketAcceptor {
    @Override
    public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
        final var session = new Session();
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
                            return Mono.just(DefaultPayload.create("single_response"));
                        case "channel.join":
                            return Mono.just(EmptyPayload.INSTANCE);
                        case "channel.leave":
                            return Mono.just(EmptyPayload.INSTANCE);
                        case "message":
                            session.messages.add("a hard coded message");
                            return Mono.just(EmptyPayload.INSTANCE);
                    }

                    return RoutingRSocket.super.requestResponse(route, payload);
                });
            }

            public Flux<Payload> requestStream(String route, Payload payload) {
                return Flux.defer(() -> {
                    switch (route) {
                        case "messages.incoming":
                            return Flux.from(subscriber -> {
                                while (true) {
                                    try {
                                        final var next = session.messages.poll(20, TimeUnit.DAYS);
                                        if (next != null) {
                                            subscriber.onNext(DefaultPayload.create(next));
                                        }
                                    } catch (InterruptedException e) {
                                        throw new RuntimeException(e);
                                    }
                                }
                            });
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
}
