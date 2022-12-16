package io.rsocket.guide.step0;

import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.util.DefaultPayload;
import reactor.core.publisher.Mono;

public class Server implements SocketAcceptor {

    @Override
    public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
        return Mono.just(new RSocket() {
            public Mono<Payload> requestResponse(String route, Payload payload) {
                return Mono.just(DefaultPayload.create("Welcome to chat, " + payload.getDataUtf8()));
            }

        });
    }

}
