package io.tapdata.pdk.apis.functions.consumers;

import java.util.List;

public interface TapReadOffsetConsumer<E> {
    void accept(List<E> events, Object offsetState, Throwable error, boolean completed);
}

