package io.rsocket.pythontest;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.metadata.CompositeMetadata;
import io.rsocket.metadata.RoutingMetadata;
import io.rsocket.metadata.WellKnownMimeType;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;

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
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
        return Flux.from(payloads).collectList()
                .flatMapMany(items -> {
                    final var first = items.get(0);
                    return routingRSocket.requestChannel(requireRoute(first), Flux.fromIterable(items));
                });
    }

    @Override
    public Mono<Void> metadataPush(Payload payload) {
        return routingRSocket.metadataPush(requireRoute(payload), payload);
    }

    static String requireRoute(Payload payload) {
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
}
