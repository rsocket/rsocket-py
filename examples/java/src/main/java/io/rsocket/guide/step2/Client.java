package io.rsocket.guide.step2;

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

    private String username;

    public Client(RSocket rSocket) {
        this.rSocket = rSocket;
    }

    public Mono<Payload> login(String username) {
        this.username = username;

        final Payload payload = DefaultPayload.create(
                toByteBuffer(username),
                composite("login")
        );
        return rSocket.requestResponse(payload);
    }

    private static CompositeByteBuf composite(String route) {
        final var metadata = ByteBufAllocator.DEFAULT.compositeBuffer();

        CompositeMetadataCodec.encodeAndAddMetadata(
                metadata,
                ByteBufAllocator.DEFAULT,
                WellKnownMimeType.MESSAGE_RSOCKET_ROUTING,
                TaggingMetadataCodec.createTaggingContent(ByteBufAllocator.DEFAULT, List.of(route))
        );

        return metadata;
    }

    private static ByteBuf toByteBuffer(String message) {
        return Unpooled.wrappedBuffer(message.getBytes());
    }
}
