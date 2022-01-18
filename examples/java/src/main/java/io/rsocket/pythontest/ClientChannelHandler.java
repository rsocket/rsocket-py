package io.rsocket.pythontest;

import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class ClientChannelHandler implements Publisher<Payload>, Subscription, Subscriber<Payload> {


    private Subscriber<Payload> subscriber;

    private final Payload request;

    private String lastMessage;

    private boolean routingRequestSent = false;

    public ClientChannelHandler(Payload request) {

        this.request = request;
    }

    @Override
    public void subscribe(Subscriber subscriber) {
        this.subscriber = subscriber;
        subscriber.onSubscribe(this);
    }

    @Override
    public void request(long l) {
        System.out.println("Received request for " + l + " Frames");
        if (!routingRequestSent) {
            subscriber.onNext(request);
            routingRequestSent = true;
        } else {
            subscriber.onNext(DefaultPayload.create(Unpooled.wrappedBuffer(("from client: " + lastMessage).getBytes())));
        }
    }

    @Override
    public void cancel() {
        System.out.println("Canceled");
    }

    @Override
    public void onSubscribe(Subscription s) {

    }

    @Override
    public void onNext(Payload payload) {
        lastMessage = payload.getDataUtf8();
        System.out.println("Response from server stream :: " + lastMessage);

        if (lastMessage.endsWith("2")) {
            subscriber.onComplete();
        }
    }

    @Override
    public void onError(Throwable t) {
        System.out.println("Error from server" + t.getMessage());
    }

    @Override
    public void onComplete() {
        System.out.println("Complete from server");
    }
}
