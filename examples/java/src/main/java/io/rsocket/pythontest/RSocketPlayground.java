package io.rsocket.pythontest;

public class RSocketPlayground {
    public static void main(String[] args) {
        Server.start();
//        var rSocket = RSocketConnector.connectWith(TcpClientTransport.create(6565))
//                .block();
    }
}
