package io.rsocket.guide.step5;

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
import io.rsocket.util.EmptyPayload;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
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
                toByteBuffer(username),
                composite("login")
        );
        return rSocket.requestResponse(payload);
    }

    public Mono<Payload> sendMessage(String data) {
        final Payload payload = DefaultPayload.create(toByteBuffer(data),
                composite("message")
        );
        return rSocket.requestResponse(payload);
    }

    public void listenForMessages() {
        new Thread(() ->
        {
            Disposable subscribe = rSocket.requestStream(DefaultPayload.create(
                            Unpooled.EMPTY_BUFFER,
                            composite("messages.incoming")))
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

    private static CompositeByteBuf composite(String route, String filename) {
        final var metadata = ByteBufAllocator.DEFAULT.compositeBuffer();

        CompositeMetadataCodec.encodeAndAddMetadata(
                metadata,
                ByteBufAllocator.DEFAULT,
                WellKnownMimeType.MESSAGE_RSOCKET_ROUTING,
                TaggingMetadataCodec.createTaggingContent(ByteBufAllocator.DEFAULT, List.of(route))
        );

        CompositeMetadataCodec.encodeAndAddMetadata(
                metadata,
                ByteBufAllocator.DEFAULT,
                MimeTypes.fileMimeType,
                toByteBuffer(filename)
        );

        return metadata;
    }

    private static ByteBuf toByteBuffer(String message) {
        return Unpooled.wrappedBuffer(message.getBytes());
    }

    public Mono<Void> upload(String filename, ByteBuf data) {
        return rSocket.requestResponse(DefaultPayload.create(data, composite("file.upload", filename))).then();
    }

    public Mono<ByteBuf> download(String filename) {
        return rSocket.requestResponse(DefaultPayload.create(Unpooled.EMPTY_BUFFER, composite("file.download", filename)))
                .map(Payload::sliceData);
    }

    public Flux<String> fileList() {
        return rSocket.requestStream(EmptyPayload.INSTANCE).map(Payload::getDataUtf8);
    }
}
