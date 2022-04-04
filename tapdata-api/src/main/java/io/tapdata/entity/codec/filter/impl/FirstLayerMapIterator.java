package io.tapdata.entity.codec.filter.impl;

import io.tapdata.entity.codec.filter.MapIterator;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public class FirstLayerMapIterator implements MapIterator {
    @Override
    public void iterate(Map<String, Object> map, Consumer<Map.Entry<String, Object>> consumer) {
        Set<Map.Entry<String, Object>> entrySet = map.entrySet();
        for(Map.Entry<String, Object> entry : entrySet) {
            consumer.accept(entry);
        }
    }
}
