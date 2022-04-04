package io.tapdata.entity.codec.filter.impl;

import io.tapdata.entity.codec.filter.MapIterator;

import java.util.*;
import java.util.function.Consumer;

/**
 * {
 *     "a" : "1",
 *     "b" : {
 *         "c" : 1,
 *         "d" : {
 *             "e" : 1
 *         }
 *     }
 * }
 * keys are below
 * a
 * b
 * b.c
 * b.d
 * b.d.e
 *
 */
public class AllLayerMapIterator implements MapIterator {
    @Override
    public void iterate(Map<String, Object> map, Consumer<Map.Entry<String, Object>> consumer) {
        Set<Map.Entry<String, Object>> entrySet = map.entrySet();
        for(Map.Entry<String, Object> entry : entrySet) {
            if(entry.getValue() instanceof Map) {
                Map<String, Object> mapFlat = new LinkedHashMap<>();
                iterateWithPrefix(entry.getKey() + ".",(Map<String, Object>) entry.getValue(),mapFlat);
                entry.setValue(mapFlat);
            }
            consumer.accept(entry);
        }
   }

    private void iterateWithPrefix(String prefix, Map<String, Object> obj,Map<String, Object> mapFlat) {
        for (Map.Entry<String, Object> entry : obj.entrySet()) {
            if(entry.getValue() instanceof Map) {
                iterateWithPrefix(prefix + entry.getKey() + ".", (Map<String, Object>) entry.getValue(),mapFlat);
            } else{
                mapFlat.put(prefix + entry.getKey(),entry.getValue());
            }
        }
    }
}
