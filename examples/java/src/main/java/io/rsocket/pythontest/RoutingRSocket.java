package io.rsocket.pythontest;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface RoutingRSocket {
    default Mono<Void> fireAndForget(String route, Payload payload) {
        return new RSocket() {
        }.fireAndForget(payload);
    }

    default Mono<Payload> requestResponse(String route, Payload payload) {
        return new RSocket() {
        }.requestResponse(payload);
    }

    default Flux<Payload> requestStream(String route, Payload payload) {
        return new RSocket() {
        }.requestStream(payload);
    }

    default Mono<Void> metadataPush(String route, Payload payload) {
        return new RSocket() {
        }.metadataPush(payload);
    }
}
