package io.rsocket.pythontest;

import java.util.StringJoiner;
import java.util.stream.IntStream;

public class Fixtures {
    public static String largeData() {
        final var joiner = new StringJoiner("");

        IntStream.range(0, 50)
                .mapToObj(i -> i + "123456789")
                .forEach(joiner::add);

        return joiner.toString();
    }
}
