package io.rsocket.guide;

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

import java.time.Duration;
import java.util.List;

public class Client {

    private RSocket rSocket;

    public Client(RSocket rSocket) {
        this.rSocket = rSocket;
    }

    public void login(String username) {
        final Payload payload = DefaultPayload.create(getPayload(username),
                composite(route("login")
                ));
        rSocket.requestResponse(payload)
                .doOnNext(response -> System.out.println("Response from server :: " + response.getDataUtf8()))
                .block(Duration.ofMinutes(10));
    }

    private static CompositeItem route(String route) {
        final var routes = List.of(route);
        final var routingMetadata = TaggingMetadataCodec.createTaggingContent(ByteBufAllocator.DEFAULT, routes);
        return new CompositeItem(routingMetadata, WellKnownMimeType.MESSAGE_RSOCKET_ROUTING);

    }
    private static CompositeByteBuf composite(Client.CompositeItem... parts) {
        final var metadata = ByteBufAllocator.DEFAULT.compositeBuffer();
        for (Client.CompositeItem part : parts) {
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

    private static ByteBuf getPayload(String message) {
        return Unpooled.wrappedBuffer(message.getBytes());
    }
}
