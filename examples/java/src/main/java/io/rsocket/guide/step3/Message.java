package io.rsocket.guide.step3;

public class Message {
    public String user;
    public String content;

    public Message() {

    }

    public Message(String user, String content) {
        this.user = user;
        this.content = content;
    }
}
