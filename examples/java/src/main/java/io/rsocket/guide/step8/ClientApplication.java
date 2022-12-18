package io.rsocket.guide.step8;

import io.netty.buffer.Unpooled;
import io.rsocket.core.RSocketConnector;
import io.rsocket.metadata.WellKnownMimeType;
import io.rsocket.transport.netty.client.TcpClientTransport;

import java.nio.charset.StandardCharsets;

public class ClientApplication {

    public static void main(String[] args) throws InterruptedException {
        final var rSocket = RSocketConnector.create()
                .fragment(64)
                .dataMimeType(WellKnownMimeType.TEXT_PLAIN.getString())
                .metadataMimeType(WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA.getString())
                .connect(TcpClientTransport.create("localhost", getPort(args)))
                .block();

        final var client = new Client(rSocket);
        messagingTest(client);
        statisticsTest(client);
        filesTest(client);
    }

    private static void filesTest(Client client) {
        String fileContent = "Content";

        client.upload("test-file1.txt", Unpooled.wrappedBuffer(fileContent.getBytes()))
                .then(client.fileList().doOnNext(System.out::println).collectList())
                .then(client.download("test-file1.txt").doOnNext(download -> {
                    if (!download.toString(StandardCharsets.UTF_8).equals(fileContent)) {
                        throw new RuntimeException("Mismatch file content");
                    }
                }))
                .block();
    }

    private static void statisticsTest(Client client) throws InterruptedException {
        client.statistics(new StatisticsSettings());
        Thread.sleep(10000);
        client.incomingStatistics.get().dispose();
    }

    private static void messagingTest(Client client) throws InterruptedException {
        client.login("user1");
        client.join("channel1");
//        client.join("channel1");

        System.out.println(client.listUsers("channel1").block());
        client.listenForMessages();

        client.sendMessage("{\"user\":\"user1\", \"content\":\"message\"}");
        client.sendMessage("{\"channel\":\"channel1\", \"content\":\"message\"}");

        client.leave("channel1");
        Thread.sleep(1000);
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
