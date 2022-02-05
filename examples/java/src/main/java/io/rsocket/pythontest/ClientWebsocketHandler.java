package io.rsocket.pythontest;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.util.ReferenceCountUtil;
import reactor.core.publisher.MonoProcessor;

import java.nio.charset.StandardCharsets;

public class ClientWebsocketHandler extends ChannelInboundHandlerAdapter {
    private final MonoProcessor<Channel> channel = MonoProcessor.create();
    private final MonoProcessor<String> pong = MonoProcessor.create();

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof PongWebSocketFrame) {
            pong.onNext(((PongWebSocketFrame) msg).content().toString(StandardCharsets.UTF_8));
            ReferenceCountUtil.safeRelease(msg);
            ctx.read();
        } else {
            super.channelRead(ctx, msg);
        }
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        Channel ch = ctx.channel();
        if (!channel.isTerminated() && ch.isWritable()) {
            channel.onNext(ctx.channel());
        }
        super.channelWritabilityChanged(ctx);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        Channel ch = ctx.channel();
        if (ch.isWritable()) {
            channel.onNext(ch);
        }
        super.handlerAdded(ctx);
    }

}
