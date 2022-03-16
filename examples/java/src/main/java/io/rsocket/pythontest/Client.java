package io.rsocket.pythontest;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.metadata.AuthMetadataCodec;
import io.rsocket.metadata.CompositeMetadataCodec;
import io.rsocket.metadata.TaggingMetadataCodec;
import io.rsocket.metadata.WellKnownMimeType;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;

import java.time.Duration;
import java.util.List;

public class Client {

    public static void main(String[] args) {
        final var rSocket = RSocketConnector.create()
                .dataMimeType(WellKnownMimeType.TEXT_PLAIN.getString())
                .metadataMimeType(WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA.getString())
                .connect(TcpClientTransport.create("localhost", getPort(args)))
                .block();

        assert rSocket != null;

        testSingleRequest(rSocket);
        testStream(rSocket);
        testStreamWithLimit(rSocket);
        testFireAndForget(rSocket);
        testChannel(rSocket);
    }

    private static int getPort(String[] args) {
        if (args.length > 0) {
            return Integer.parseInt(args[0]);
        } else {
            return 6565;
        }
    }

    private static void testStreamWithLimit(RSocket rSocket) {
        rSocket.requestStream(DefaultPayload.create(getPayload("simple"),
                        composite(route("stream"), authenticate("simple", "12345"))))
                .limitRate(1)
                .doOnComplete(() -> System.out.println("Response from server stream completed"))
                .doOnNext(response -> System.out.println("Response from server stream :: " + response.getDataUtf8()))
                .collectList()
                .block(Duration.ofMinutes(5));
    }

    private static void testChannel(RSocket rSocket) {
        final var payload = DefaultPayload.create(getPayload("simple"),
                composite(route("channel"), authenticate("simple", "12345")));
        final var channel = new ClientChannelHandler(payload);

        rSocket.requestChannel(channel)
                .limitRate(1)
                .doOnNext(channel::onNext)
                .doOnComplete(channel::onComplete)
                .collectList()
                .block(Duration.ofMinutes(1));
    }

    private static void testFireAndForget(RSocket rSocket) {
        rSocket.fireAndForget(DefaultPayload.create(getPayload("simple"),
                        composite(route("no_response"), authenticate("simple", "12345"))))
                .block(Duration.ofMinutes(5));
    }

    private static void testStream(RSocket rSocket) {
        rSocket.requestStream(DefaultPayload.create(getPayload("simple"),
                        composite(route("stream"), authenticate("simple", "12345"))))
                .doOnComplete(() -> System.out.println("Response from server stream completed"))
                .doOnNext(response -> System.out.println("Response from server stream :: " + response.getDataUtf8()))
                .collectList()
                .block(Duration.ofMinutes(5));
    }

    private static void testSingleRequest(RSocket rSocket) {
        rSocket.requestResponse(DefaultPayload.create(getPayload("simple stream"),
                        composite(route("single_request"), authenticate("simple", "12345"))))
                .doOnNext(response -> System.out.println("Response from server :: " + response.getDataUtf8()))
                .block(Duration.ofMinutes(5));
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

    private static CompositeItem route(String route) {
        final var routes = List.of(route);
        final var routingMetadata = TaggingMetadataCodec.createRoutingMetadata(ByteBufAllocator.DEFAULT, routes);
        return new CompositeItem(routingMetadata.getContent(), WellKnownMimeType.MESSAGE_RSOCKET_ROUTING);

    }

    private static CompositeItem authenticate(String username, String password) {

        final var authMetadata = AuthMetadataCodec.encodeSimpleMetadata(ByteBufAllocator.DEFAULT,
                username.toCharArray(), password.toCharArray());
        return new CompositeItem(authMetadata, WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION);
    }

    private static ByteBuf getPayload(String message) {
        return Unpooled.wrappedBuffer(message.getBytes());
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
