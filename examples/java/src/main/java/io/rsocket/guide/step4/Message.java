package io.rsocket.guide.step4;

public class Message {
    public String user;
    public String content;
    public String channel;

    public Message() {

    }

    public Message(String user, String content) {
        this.user = user;
        this.content = content;
    }

    public Message(String user, String content, String channel) {
        this.user = user;
        this.content = content;
        this.channel = channel;
    }
}
