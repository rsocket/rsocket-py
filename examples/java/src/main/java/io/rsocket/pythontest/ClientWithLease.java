package io.rsocket.pythontest;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.metadata.CompositeMetadataCodec;
import io.rsocket.metadata.TaggingMetadataCodec;
import io.rsocket.metadata.WellKnownMimeType;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ClientWithLease {

    public static void main(String[] args) throws InterruptedException {
        final var rSocket = RSocketConnector.create()
                .lease()
                .dataMimeType(WellKnownMimeType.TEXT_PLAIN.getString())
                .metadataMimeType(WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA.getString())
                .connect(TcpClientTransport.create("localhost", 6565))
                .block();

        assert rSocket != null;

        testSingleRequest(rSocket);

        tryIgnoringLease(rSocket);

        rSocket.dispose();

    }

    private static void tryIgnoringLease(RSocket rSocket) throws InterruptedException {
        TimeUnit.SECONDS.sleep(3);

        boolean errorThrown = false;
        try {
            testSingleRequest(rSocket);

        } catch (Exception exception) {
            errorThrown = true;
        }

        if (!errorThrown) {
            throw new RuntimeException("Lease not honored");
        }
    }

    private static void testSingleRequest(RSocket rSocket) {
        rSocket.requestResponse(DefaultPayload.create(getPayload("simple stream"),
                        composite(route("single_request"))))
                .doOnNext(response -> System.out.println("Response from server : " + response.getDataUtf8()))
                .doOnError(error -> System.out.println("Error from server : " + error.getMessage()))
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
