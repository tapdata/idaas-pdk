package io.tapdata.pdk.apis.functions.consumers;


public interface TapReadOneConsumer<E> {
    void accept(E event, Throwable error);
}
