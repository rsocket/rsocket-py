package io.rsocket.guide.step4;

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
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class Client {

    private final RSocket rSocket;

    private String username;

    public final AtomicReference<Disposable> incomingMessages = new AtomicReference<>();

    public Client(RSocket rSocket) {
        this.rSocket = rSocket;
    }

    public Mono<Payload> login(String username) {
        this.username = username;

        final Payload payload = DefaultPayload.create(
                Unpooled.wrappedBuffer(username.getBytes()),
                route("login")
        );
        return rSocket.requestResponse(payload);
    }

    public Mono<Payload> sendMessage(String data) {
        final Payload payload = DefaultPayload.create(Unpooled.wrappedBuffer(data.getBytes()),
                route("message")
        );
        return rSocket.requestResponse(payload);
    }

    public void listenForMessages() {
        new Thread(() ->
        {
            Disposable subscribe = rSocket.requestStream(DefaultPayload.create(null, route("messages.incoming")))
                    .doOnComplete(() -> System.out.println("Response from server stream completed"))
                    .doOnNext(response -> System.out.println("Response from server stream :: " + response.getDataUtf8()))

                    .collectList()
                    .subscribe();
            incomingMessages.set(subscribe);
        }).start();
    }

    public void stopListeningForMessages() {
        incomingMessages.get().dispose();
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