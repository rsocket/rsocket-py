package io.rsocket.guide.step1;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.metadata.CompositeMetadataCodec;
import io.rsocket.metadata.TaggingMetadataCodec;
import io.rsocket.metadata.WellKnownMimeType;
import io.rsocket.util.DefaultPayload;
import reactor.core.publisher.Mono;

import java.util.List;

public class Client {

    private final RSocket rSocket;

    public Client(RSocket rSocket) {
        this.rSocket = rSocket;
    }

    public Mono<Payload> login(String username) {
        final Payload payload = DefaultPayload.create(
                Unpooled.wrappedBuffer(username.getBytes()),
                route("login")
        );
        return rSocket.requestResponse(payload);
    }

    private static CompositeByteBuf route(String route) {
        final var metadata = ByteBufAllocator.DEFAULT.compositeBuffer();

        CompositeMetadataCodec.encodeAndAddMetadata(
                metadata,
                ByteBufAllocator.DEFAULT,
                WellKnownMimeType.MESSAGE_RSOCKET_ROUTING,
                TaggingMetadataCodec.createTaggingContent(ByteBufAllocator.DEFAULT, List.of(route))
        );

        return metadata;
    }
}
