package io.tapdata.pdk.apis.functions.consumers;

import java.util.List;

public interface TapListConsumer<E> {
    void accept(List<E> events, Throwable error);
}
