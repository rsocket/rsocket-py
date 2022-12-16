package io.rsocket.guide.step8;

import io.rsocket.core.RSocketServer;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.netty.server.TcpServerTransport;

import java.util.Objects;

public class ServerApplication {

    public static void main(String[] args) {
        int port = getPort(args);
        System.out.println("Port: " + port);
        RSocketServer rSocketServer = RSocketServer.create();
        rSocketServer.acceptor(new Server());
        rSocketServer.payloadDecoder(PayloadDecoder.ZERO_COPY);
        Objects.requireNonNull(rSocketServer.bind(TcpServerTransport.create(port))
                        .block())
                .onClose()
                .block();
    }

    private static int getPort(String[] args) {
        if (args.length > 0) {
            return Integer.parseInt(args[0]);
        } else {
            return 6565;
        }
    }

}
