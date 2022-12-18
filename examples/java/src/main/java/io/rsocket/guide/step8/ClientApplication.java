package io.rsocket.guide.step8;

import io.netty.buffer.Unpooled;
import io.rsocket.core.RSocketConnector;
import io.rsocket.metadata.WellKnownMimeType;
import io.rsocket.transport.netty.client.TcpClientTransport;
import reactor.core.publisher.Mono;

import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

public class ClientApplication {

    public static void main(String[] args) throws InterruptedException {
        final var rSocket1 = RSocketConnector.create()
                .fragment(64)
                .dataMimeType(WellKnownMimeType.TEXT_PLAIN.getString())
                .metadataMimeType(WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA.getString())
                .connect(TcpClientTransport.create("localhost", getPort(args)))
                .block();

        final var rSocket2 = RSocketConnector.create()
                .fragment(64)
                .dataMimeType(WellKnownMimeType.TEXT_PLAIN.getString())
                .metadataMimeType(WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA.getString())
                .connect(TcpClientTransport.create("localhost", getPort(args)))
                .block();

        final var client1 = new Client(rSocket1);
        final var client2 = new Client(rSocket2);
        messagingTest(client1, client2);
        statisticsTest(client1);
        filesTest(client1);
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

    private static void messagingTest(Client client1, Client client2) throws InterruptedException {
        client1.login("user1")
                .then(Mono.defer(() -> client1.join("channel1")))
                .then(Mono.defer(() -> client2.login("user2")))
                .then(Mono.defer(() -> client2.join("channel1")))
                .then(Mono.defer(() -> client1.listUsers("channel1")
                        .map(users -> printList(users))))
                .block();

        client1.listenForMessages();
        client2.listenForMessages();

        client1.sendMessage(new Message("user1", "message"));
        client1.sendMessage(new Message(null, "message", "channel1"));

        client1.leave("channel1").block();
        Thread.sleep(1000);
        client1.incomingMessages.get().dispose();
        client2.incomingMessages.get().dispose();
    }

    private static PrintStream printList(List<String> strings) {
        return System.out.printf(strings.stream().collect(Collectors.joining(",")));
    }

    private static int getPort(String[] args) {
        if (args.length > 0) {
            return Integer.parseInt(args[0]);
        } else {
            return 6565;
        }
    }
}
