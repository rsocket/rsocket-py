package io.rsocket.pythontest;

import io.netty.channel.ChannelHandler;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import io.rsocket.util.DefaultPayload;
import reactor.netty.http.client.HttpClient;

public class ClientWebsocket {
    private static final String host = "localhost";
    private static final int port = 6565;


    public static void main(String[] args) {

        ChannelHandler pingSender = new ClientWebsocketHandler();

        HttpClient httpClient =
                HttpClient.create()
                        .doOnConnected(b -> b.addHandlerLast(pingSender))
                        .host(host)
                        .port(port);

        RSocket rSocket =
                RSocketConnector.connectWith(WebsocketClientTransport.create(httpClient, "/")).block();


        rSocket.requestResponse(DefaultPayload.create("ping")).block();
    }
}
