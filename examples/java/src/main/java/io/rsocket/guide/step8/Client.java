package io.rsocket.guide.step8;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class Client {

    private final RSocket rSocket;

    private final ObjectMapper objectMapper = new ObjectMapper();

    final public BlockingQueue<StatisticsSettings> statisticsSettings = new LinkedBlockingQueue<>();

    private String username;

    public final AtomicReference<Disposable> incomingMessages = new AtomicReference<>();
    public final AtomicReference<Disposable> incomingStatistics = new AtomicReference<>();
    private final AtomicReference<Thread> threadContainer = new AtomicReference<>();

    public Client(RSocket rSocket) {
        this.rSocket = rSocket;
    }

    public void login(String username) {
        this.username = username;

        final Payload payload = DefaultPayload.create(toByteBuffer(username),
                composite("login")
        );
        rSocket.requestResponse(payload)
                .doOnNext(response -> System.out.println("Response from server :: " + response.getDataUtf8()))
                .block(Duration.ofMinutes(10));
    }

    public void join(String channel) {
        final Payload payload = DefaultPayload.create(toByteBuffer(channel),
                composite("channel.join")
        );
        rSocket.requestResponse(payload)
                .block(Duration.ofMinutes(10));
    }

    public void leave(String channel) {
        final Payload payload = DefaultPayload.create(toByteBuffer(channel),
                composite("channel.leave")
        );
        rSocket.requestResponse(payload)
                .block(Duration.ofMinutes(10));
    }

    public void statistics(StatisticsSettings settings) {
        new Thread(() -> {
            final var payloads = Flux.concat(
                    Flux.just(settings).map(e -> {
                        try {
                            return DefaultPayload.create(toByteBuffer(objectMapper.writeValueAsString(e)), composite("statistics"));
                        } catch (JsonProcessingException exception) {
                            throw new RuntimeException(exception);
                        }
                    }),
                    Flux.create(sink -> sink
                            .onRequest(n -> {
                                if (threadContainer.get() == null) {
                                    final var thread = new Thread(() -> sendSettings(sink));
                                    thread.start();
                                    threadContainer.set(thread);
                                }
                            }).onCancel(() -> threadContainer.get().interrupt())
                            .onDispose(() -> threadContainer.get().interrupt()))
            );
            incomingStatistics.set(rSocket.requestChannel(payloads)
                    .doOnNext(response -> System.out.println("Response from server stream :: " + response.getDataUtf8()))
                    .subscribe());
        }).start();
    }

    private <T> Payload toJson(T e) {
        try {
            return DefaultPayload.create(objectMapper.writeValueAsString(e));
        } catch (JsonProcessingException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void listenForMessages() {
        new Thread(() ->
                incomingMessages.set(rSocket.requestStream(DefaultPayload.create(toByteBuffer("simple"),
                                composite("messages.incoming")))
                        .doOnComplete(() -> System.out.println("Response from server stream completed"))
                        .doOnNext(response -> System.out.println("Response from server stream :: " + response.getDataUtf8()))
                        .subscribe()))
                .start();
    }

    public void sendSettings(FluxSink<Payload> sink) {
        while (true) {
            try {
                final var next = statisticsSettings.poll(20, TimeUnit.DAYS);
                if (next != null) {
                    sink.next(DefaultPayload.create(toJson(next)));
                }
            } catch (Exception exception) {
                break;
            }
        }
    }

    public void sendMessage(String data) {
        final Payload payload = DefaultPayload.create(toByteBuffer(data),
                composite("message")
        );
        rSocket.requestResponse(payload)
                .doOnError(e -> System.out.println(e.getMessage()))
                .block(Duration.ofMinutes(10));
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

    public Mono<List<String>> listUsers(String channel) {
        return rSocket.requestStream(DefaultPayload.create(toByteBuffer(channel),
                        composite("channel.users")))
                .map(Payload::getDataUtf8).collectList();
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
