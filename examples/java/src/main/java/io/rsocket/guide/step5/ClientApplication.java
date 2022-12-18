package io.rsocket.guide.step5;

import io.rsocket.core.RSocketConnector;
import io.rsocket.metadata.WellKnownMimeType;
import io.rsocket.transport.netty.client.TcpClientTransport;

import java.time.Duration;

public class ClientApplication {

    public static void main(String[] args) throws InterruptedException {
        final var transport = TcpClientTransport.create("localhost", 6565);

        final var rSocket = RSocketConnector.create()
                .metadataMimeType(WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA.getString())
                .connect(transport)
                .block();

        final var client = new Client(rSocket);

        client.login("George")
                .doOnNext(response -> System.out.println(response.getDataUtf8()))
                .block(Duration.ofMinutes(10));

        client.listenForMessages();
        client.sendMessage("{\"user\":\"user1\", \"content\":\"message\"}");
        Thread.sleep(2000);
        client.incomingMessages.get().dispose();
    }
}