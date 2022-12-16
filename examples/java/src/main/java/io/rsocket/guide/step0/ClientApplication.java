package io.rsocket.guide.step0;

import io.rsocket.Payload;
import io.rsocket.core.RSocketConnector;
import io.rsocket.metadata.WellKnownMimeType;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;

import java.time.Duration;

public class ClientApplication {

    public static void main(String[] args) throws InterruptedException {
        final var rSocket = RSocketConnector.create()
                .connect(TcpClientTransport.create("localhost", getPort(args)))
                .block();

        final Payload payload = DefaultPayload.create("George");
        rSocket.requestResponse(payload)
                .doOnNext(response -> System.out.println(response.getDataUtf8()))
                .block(Duration.ofMinutes(1));
    }

    private static int getPort(String[] args) {
        if (args.length > 0) {
            return Integer.parseInt(args[0]);
        } else {
            return 6565;
        }
    }
}
