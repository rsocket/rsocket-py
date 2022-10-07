package io.rsocket.pythontest;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.metadata.RoutingMetadata;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RoutingRSocketAdapter implements RSocket {

    private final RoutingRSocket routingRSocket;

    public RoutingRSocketAdapter(final RoutingRSocket routingRSocket) {
        this.routingRSocket = routingRSocket;
    }

    @Override
    public Mono<Void> fireAndForget(Payload payload) {
        return routingRSocket.fireAndForget(requireRoute(payload), payload);
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
        return routingRSocket.requestResponse(requireRoute(payload), payload);
    }

    @Override
    public Flux<Payload> requestStream(Payload payload) {
        return routingRSocket.requestStream(requireRoute(payload), payload);
    }

    @Override
    public Mono<Void> metadataPush(Payload payload) {
        return routingRSocket.metadataPush(requireRoute(payload), payload);
    }

    private static String requireRoute(Payload payload) {
        final String route;
        if (payload.hasMetadata()) { // check if you have compulsory metadata
            route = new RoutingMetadata(payload.metadata().slice()).iterator().next(); // read the routing metadata value
        } else {
            throw new IllegalStateException();
        }
        return route;
    }
}
