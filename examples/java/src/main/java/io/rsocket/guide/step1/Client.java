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
        final Payload payload = DefaultPayload.create(Unpooled.wrappedBuffer(username.getBytes()),
                composite(route("login")
                ));
        return rSocket.requestResponse(payload);
    }

    private static CompositeItem route(String route) {
        final var routes = List.of(route);
        final var routingMetadata = TaggingMetadataCodec.createTaggingContent(ByteBufAllocator.DEFAULT, routes);
        return new CompositeItem(routingMetadata, WellKnownMimeType.MESSAGE_RSOCKET_ROUTING);

    }

    private static CompositeByteBuf composite(CompositeItem... parts) {
        final var metadata = ByteBufAllocator.DEFAULT.compositeBuffer();
        for (CompositeItem part : parts) {
            CompositeMetadataCodec.encodeAndAddMetadata(
                    metadata,
                    ByteBufAllocator.DEFAULT,
                    part.mimeType,
                    part.data
            );
        }
        return metadata;
    }

    private static class CompositeItem {
        public ByteBuf data;
        public WellKnownMimeType mimeType;

        public CompositeItem(ByteBuf data, WellKnownMimeType mimeType) {
            this.data = data;
            this.mimeType = mimeType;
        }
    }
}
