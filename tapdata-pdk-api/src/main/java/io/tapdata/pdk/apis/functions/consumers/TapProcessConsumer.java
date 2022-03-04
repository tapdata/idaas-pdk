package io.tapdata.pdk.apis.functions.consumers;

import java.util.List;

public interface TapProcessConsumer<E> {
    void accept(List<E> events, Throwable throwable);
}
