package io.rsocket.guide.step0;

import io.rsocket.core.RSocketConnector;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;

import java.time.Duration;

public class ClientApplication {

    public static void main(String[] args) {
        final var transport = TcpClientTransport.create("localhost", 6565);

        final var rSocket = RSocketConnector.create()
                .connect(transport)
                .block();

        final var payload = DefaultPayload.create("George");

        rSocket.requestResponse(payload)
                .doOnNext(response -> System.out.println(response.getDataUtf8()))
                .block(Duration.ofMinutes(1));
    }
}
