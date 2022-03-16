package io.tapdata.entity.codec.filter;

import java.util.Map;
import java.util.function.Consumer;

public interface MapIterator<V> {
    void iterate(Map<String, V> map, Consumer<Map.Entry<String, V>> consumer);
}
