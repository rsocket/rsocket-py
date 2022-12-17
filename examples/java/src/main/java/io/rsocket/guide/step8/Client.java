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
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

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
    private AtomicReference<Thread> threadContainer = new AtomicReference<>();

    public Client(RSocket rSocket) {
        this.rSocket = rSocket;
    }

    public void login(String username) {
        this.username = username;

        final Payload payload = DefaultPayload.create(getPayload(username),
                route("login")
        );
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

    public void statistics(StatisticsSettings settings) {
        new Thread(() -> {
            final var payloads = Flux.concat(
                    Flux.just(settings).map(e -> {
                        try {
                            return DefaultPayload.create(getPayload(objectMapper.writeValueAsString(e)), route("statistics"));
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
                incomingMessages.set(rSocket.requestStream(DefaultPayload.create(getPayload("simple"),
                                route("messages.incoming")))
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
        final Payload payload = DefaultPayload.create(getPayload(data),
                route("message")
        );
        rSocket.requestResponse(payload)
                .doOnNext(response -> System.out.println("Response from server :: " + response.getDataUtf8()))
                .block(Duration.ofMinutes(10));
    }

    private void sendStringToRoute(String username, String route) {
        final Payload payload = DefaultPayload.create(getPayload(username),
                route(route)
        );
        rSocket.requestResponse(payload)
                .doOnNext(response -> System.out.println("Response from server :: " + response.getDataUtf8()))
                .block(Duration.ofMinutes(10));
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

    public List<String> listUsers(String channel) {
        return rSocket.requestStream(DefaultPayload.create(getPayload(channel),
                        route("channel.users")))
                .map(Payload::getDataUtf8).collectList().block();
    }

    private static ByteBuf getPayload(String message) {
        return Unpooled.wrappedBuffer(message.getBytes());
    }
}
