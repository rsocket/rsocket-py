package io.rsocket.pythontest;

import io.rsocket.core.RSocketServer;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.netty.server.TcpServerTransport;

import java.util.Objects;

public class Server {

    public static void start() {
        RSocketServer rSocketServer = RSocketServer.create();
        rSocketServer.acceptor(new SimpleRSocketAcceptor());
        rSocketServer.payloadDecoder(PayloadDecoder.ZERO_COPY);
        Objects.requireNonNull(rSocketServer.bind(TcpServerTransport.create(6565))
                        .block())
                .onClose()
                .block();
    }

}
