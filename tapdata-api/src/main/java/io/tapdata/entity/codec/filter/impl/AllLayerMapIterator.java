package io.tapdata.entity.codec.filter.impl;

import io.tapdata.entity.codec.filter.MapIterator;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public class AllLayerMapIterator<T> implements MapIterator<T> {
    @Override
    public void iterate(Map<String, T> map, Consumer<Map.Entry<String, T>> consumer) {
        //TODO
//        Set<Map.Entry<String, T>> entrySet = map.entrySet();
//        for(Map.Entry<String, T> entry : entrySet) {
//            consumer.accept(entry);
//        }
    }
}
