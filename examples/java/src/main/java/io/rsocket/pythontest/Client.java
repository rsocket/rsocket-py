package io.rsocket.pythontest;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
import io.rsocket.core.RSocketConnector;
import io.rsocket.metadata.CompositeMetadataCodec;
import io.rsocket.metadata.RoutingMetadata;
import io.rsocket.metadata.TaggingMetadataCodec;
import io.rsocket.metadata.WellKnownMimeType;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.List;

public class Client {

    public static void main(String[] args) {
        var rSocket = RSocketConnector.create()
                .dataMimeType(WellKnownMimeType.TEXT_PLAIN.getString())
                .metadataMimeType(WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA.getString())
                .connect(TcpClientTransport.create("localhost", 6565))
                .block();

        var payload = DefaultPayload.create(getPayload("simple"), route("single_request"));

        assert rSocket != null;
        rSocket.requestResponse(payload)
                .doOnNext(response -> {
                    System.out.println("Response from server :: " + response.getDataUtf8());
                }).block(Duration.ofMinutes(5));

//        getRequestPayloads("single_request")
//                .flatMap(rSocket::requestResponse)
//                .doOnNext(response -> {
//                    System.out.println("Response from server :: " + response.getDataUtf8());
//                })
//                .blockLast(Duration.ofMinutes(10));

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

    private static Flux<Payload> getRequestPayloads(String routeTag) {
        return Flux.just("hi", "hello", "how", "are", "you")
                .delayElements(Duration.ofSeconds(1))
                .map(data -> DefaultPayload.create(getPayload(data), route(routeTag)));
    }
}
