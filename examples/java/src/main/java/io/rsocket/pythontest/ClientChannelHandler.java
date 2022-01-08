package io.rsocket.pythontest;

import io.rsocket.Payload;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class ClientChannelHandler implements Subscriber<Payload>, Publisher<Payload>, Subscription {
    @Override
    public void subscribe(Subscriber subscriber) {
        subscriber.onSubscribe(this);
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        subscription.request(1);
    }

    @Override
    public void onNext(Payload payload) {

    }

    @Override
    public void onError(Throwable throwable) {

    }

    @Override
    public void onComplete() {

    }

    @Override
    public void request(long l) {

    }

    @Override
    public void cancel() {

    }
}
