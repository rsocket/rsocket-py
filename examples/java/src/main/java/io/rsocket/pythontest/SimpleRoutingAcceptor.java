package io.rsocket.pythontest;

import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.util.DefaultPayload;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.StringJoiner;
import java.util.stream.IntStream;

public class SimpleRoutingAcceptor implements SocketAcceptor {
    @Override
    public Mono<RSocket> accept(ConnectionSetupPayload connectionSetupPayload, RSocket rSocket) {
        return Mono.just(new RoutingRSocketAdapter(new RoutingRSocket() {
            public Mono<Void> fireAndForget(String route, Payload payload) {
                switch (route) {
                    case "no_response":
                        var str = payload.getDataUtf8();
                        System.out.println("Received :: " + str);
                        return Mono.empty();
                }

                return RoutingRSocket.super.fireAndForget(route, payload);
            }

            public Mono<Payload> requestResponse(String route, Payload payload) {
                switch (route) {
                    case "single_request":
                        return Mono.just(DefaultPayload.create("single_response"));
                    case "large_request":
                        return Mono.just(DefaultPayload.create(payload.getDataUtf8()));
                    case "large_data":
                        final var joiner = new StringJoiner("");

                        IntStream.range(0, 50)
                                .mapToObj(i -> i + "123456789")
                                .forEach(joiner::add);

                        return Mono.just(DefaultPayload.create(joiner.toString()));
                }

                return RoutingRSocket.super.requestResponse(route, payload);
            }

            public Flux<Payload> requestStream(String route, Payload payload) {
                switch (route) {
                    case "stream":
                        return Flux.range(0, 3)
                                .map(index -> "Item on channel: " + index)
                                .map(DefaultPayload::create);
                }

                return RoutingRSocket.super.requestStream(route, payload);
            }
        }));
    }
}
