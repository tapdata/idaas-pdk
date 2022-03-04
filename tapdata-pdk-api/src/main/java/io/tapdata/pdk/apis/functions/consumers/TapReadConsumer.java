package io.tapdata.pdk.apis.functions.consumers;

import java.util.List;

public interface TapReadConsumer<E> {
    void accept(List<E> events, Throwable error, boolean completed);
}
