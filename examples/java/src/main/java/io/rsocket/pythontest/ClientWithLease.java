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
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ClientWithLease {

    public static void main(String[] args) throws InterruptedException {
        final var rSocket = RSocketConnector.create()
                .lease(c -> c.maxPendingRequests(0))
                .dataMimeType(WellKnownMimeType.TEXT_PLAIN.getString())
                .metadataMimeType(WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA.getString())
                .connect(TcpClientTransport.create("localhost", getPort(args)))
                .block();

        assert rSocket != null;

        testSingleRequest(rSocket);

        tryIgnoringLease(rSocket);

        rSocket.dispose();
    }

    private static int getPort(String[] args) {
        if (args.length > 0) {
            return Integer.parseInt(args[0]);
        } else {
            return 6565;
        }
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
        Mono.defer(() -> rSocket.requestResponse(DefaultPayload.create(getPayload("simple stream"),
                        composite(route("single_request")))))
                .doOnNext(response -> System.out.println("Response from server : " + response.getDataUtf8()))
                .doOnError(error -> System.out.println(error.getMessage()))
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)).maxBackoff(Duration.ofSeconds(5)))
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
