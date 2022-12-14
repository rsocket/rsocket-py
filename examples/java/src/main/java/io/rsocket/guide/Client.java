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

    private final RSocket rSocket;

    public Client(RSocket rSocket) {
        this.rSocket = rSocket;
    }

    public void login(String username) {
        sendStringToRoute(username, "login");
    }

    public void join(String channel) {
        sendStringToRoute(channel, "channel.join");
    }

    public void leave(String channel) {
        sendStringToRoute(channel, "channel.leave");
    }

    public void listenForMessages() {
        new Thread(() ->
                rSocket.requestStream(DefaultPayload.create(getPayload("simple"),
                                composite(route("messages.incoming"))))
                        .doOnComplete(() -> System.out.println("Response from server stream completed"))
                        .doOnNext(response -> System.out.println("Response from server stream :: " + response.getDataUtf8()))
                        .collectList()
                        .block())
                .start();
    }

    public void sendMessage(String data) {
        sendStringToRoute(data, "message");
    }

    private void sendStringToRoute(String username, String route) {
        final Payload payload = DefaultPayload.create(getPayload(username),
                composite(route(route)
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
