package io.rsocket.pythontest;

import io.rsocket.ConnectionSetupPayload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import reactor.core.publisher.Mono;

public class SimpleRSocketAcceptor implements SocketAcceptor {

    @Override
    public Mono<RSocket> accept(ConnectionSetupPayload connectionSetupPayload, RSocket rSocket) {
        return Mono.just(new SimpleRSocket());
    }

}
