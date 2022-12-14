package io.rsocket.guide;

import io.rsocket.core.RSocketConnector;
import io.rsocket.metadata.WellKnownMimeType;
import io.rsocket.transport.netty.client.TcpClientTransport;

public class ClientApplication {

    public static void main(String[] args) throws InterruptedException {
        final var rSocket = RSocketConnector.create()
                .fragment(64)
                .dataMimeType(WellKnownMimeType.TEXT_PLAIN.getString())
                .metadataMimeType(WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA.getString())
                .connect(TcpClientTransport.create("localhost", getPort(args)))
                .block();

        final var client = new Client(rSocket);
        client.login("user1");
        client.join("channel1");
        client.join("channel1");

        client.listenForMessages();
        client.sendMessage("{\"user\":\"user1\", \"content\":\"message\"}");
        client.sendMessage("{\"channel\":\"channel1\", \"content\":\"message\"}");

        client.leave("channel1");
        Thread.sleep(2000);
        client.incomingMessages.get().dispose();
    }

    private static int getPort(String[] args) {
        if (args.length > 0) {
            return Integer.parseInt(args[0]);
        } else {
            return 6565;
        }
    }
}
