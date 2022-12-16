package io.rsocket.guide.step8;

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
import reactor.core.Disposable;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class Client {

    private final RSocket rSocket;

    private String username;

    public final AtomicReference<Disposable> incomingMessages = new AtomicReference<>();

    public Client(RSocket rSocket) {
        this.rSocket = rSocket;
    }

    public void login(String username) {
        this.username = username;

        final Payload payload = DefaultPayload.create(getPayload(username),
                composite(route("login")
                ));
        rSocket.requestResponse(payload)
                .doOnNext(response -> System.out.println("Response from server :: " + response.getDataUtf8()))
                .block(Duration.ofMinutes(10));
    }

    public void join(String channel) {
        sendStringToRoute(channel, "channel.join");
    }

    public void leave(String channel) {
        sendStringToRoute(channel, "channel.leave");
    }

    public void listenForMessages() {
        new Thread(() ->
        {
            Disposable subscribe = rSocket.requestStream(DefaultPayload.create(getPayload("simple"),
                            composite(route("messages.incoming"))))
                    .doOnComplete(() -> System.out.println("Response from server stream completed"))
                    .doOnNext(response -> System.out.println("Response from server stream :: " + response.getDataUtf8()))

                    .collectList()
                    .subscribe();
            incomingMessages.set(subscribe);
        }).start();
    }

    public void sendMessage(String data) {
        final Payload payload = DefaultPayload.create(getPayload(data),
                composite(route("message")
                ));
        rSocket.requestResponse(payload)
                .doOnNext(response -> System.out.println("Response from server :: " + response.getDataUtf8()))
                .block(Duration.ofMinutes(10));
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

    public List<String> listUsers(String channel) {
        return rSocket.requestStream(DefaultPayload.create(getPayload(channel),
                composite(route("channel.users"))))
                .map(Payload::getDataUtf8).collectList().block();
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
