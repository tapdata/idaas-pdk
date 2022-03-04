package io.tapdata.pdk.apis.functions.consumers;


public interface TapOffsetConsumer<E> {
    void accept(E event, Throwable error);
}
