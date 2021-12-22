package io.rsocket.pythontest;

import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.metadata.CompositeMetadata;
import io.rsocket.util.DefaultPayload;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;

public class SimpleRSocket implements RSocket {

    public Mono<Void> fireAndForget(Payload payload) {
        var str = payload.getDataUtf8();
        System.out.println("Received :: " + str);
        return Mono.empty();
    }

    public Mono<Payload> requestResponse(Payload payload) {
        var str = payload.getDataUtf8();
        return Mono.just(DefaultPayload.create(str.toUpperCase()));
    }

    public Flux<Payload> requestStream(Payload payload) {
        var metadata = Unpooled.wrappedBuffer(payload.getMetadata());

        var route = new ArrayList<String>();
        new CompositeMetadata(metadata, true).forEach(entry -> {
            route.add(entry.getContent().toString(CharsetUtil.US_ASCII));
        });

        var data = payload.getDataUtf8();
        return Flux.concat(Flux.fromStream(route.stream()), Flux.just(data))
                .map(DefaultPayload::create);
    }
}
