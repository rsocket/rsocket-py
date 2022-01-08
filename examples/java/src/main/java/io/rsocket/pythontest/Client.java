package io.rsocket.pythontest;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.metadata.CompositeMetadataCodec;
import io.rsocket.metadata.RoutingMetadata;
import io.rsocket.metadata.TaggingMetadataCodec;
import io.rsocket.metadata.WellKnownMimeType;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;

import java.time.Duration;
import java.util.List;

public class Client {

    public static void main(String[] args) {
        var rSocket = RSocketConnector.create()
                .dataMimeType(WellKnownMimeType.TEXT_PLAIN.getString())
                .metadataMimeType(WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA.getString())
                .connect(TcpClientTransport.create("localhost", 6565))
                .block();

        assert rSocket != null;

        testSingleRequest(rSocket);
        testStream(rSocket);
        testFireAndForget(rSocket);
//        testChannel(rSocket);
    }

    private static void testChannel(RSocket rSocket) {
        rSocket.requestChannel(new ClientChannelHandler()).blockLast(Duration.ofMinutes(5));
    }

    private static void testFireAndForget(RSocket rSocket) {
        rSocket.fireAndForget(DefaultPayload.create(getPayload("simple"), route("no_response")))
                .block(Duration.ofMinutes(5));
    }

    private static void testStream(RSocket rSocket) {
        rSocket.requestStream(DefaultPayload.create(getPayload("simple"), route("stream1")))
                .limitRate(1)
                .doOnComplete(() -> System.out.println("Response from server stream completed"))
                .doOnNext(response -> System.out.println("Response from server stream :: " + response.getDataUtf8()))
                .collectList()
                .block(Duration.ofMinutes(5));
    }

    private static void testSingleRequest(RSocket rSocket) {
        rSocket.requestResponse(DefaultPayload.create(getPayload("simple stream"), route("single_request")))
                .doOnNext(response -> System.out.println("Response from server :: " + response.getDataUtf8()))
                .block(Duration.ofMinutes(5));
    }

    private static CompositeByteBuf route(String route) {
        List<String> routes = List.of(route);
        CompositeByteBuf metadata = ByteBufAllocator.DEFAULT.compositeBuffer();
        RoutingMetadata routingMetadata = TaggingMetadataCodec.createRoutingMetadata(ByteBufAllocator.DEFAULT, routes);
        CompositeMetadataCodec.encodeAndAddMetadata(
                metadata,
                ByteBufAllocator.DEFAULT,
                WellKnownMimeType.MESSAGE_RSOCKET_ROUTING,
                routingMetadata.getContent()
        );
        return metadata;
    }

    private static ByteBuf getPayload(String message) {
        return Unpooled.wrappedBuffer(message.getBytes());
    }

}
