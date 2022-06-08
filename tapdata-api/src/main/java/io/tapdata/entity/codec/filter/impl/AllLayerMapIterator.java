package io.tapdata.entity.codec.filter.impl;

import io.tapdata.entity.codec.filter.MapIteratorEx;

import java.util.*;
import java.util.function.BiFunction;

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
public class AllLayerMapIterator implements MapIteratorEx {
//    @Override
//    public void iterate(Map<String, Object> map, Consumer<Map.Entry<String, Object>> consumer) {
//        Set<Map.Entry<String, Object>> entrySet = map.entrySet();
//        for(Map.Entry<String, Object> entry : entrySet) {
//            if(entry.getValue() instanceof Map) {
//                iterateWithPrefix(entry.getKey() + ".", (Map<String, Object>) entry.getValue(), consumer);
//            } else if(entry.getValue() instanceof Collection) {
//                iterateListWithPrefix(entry.getKey() + ".#", (Collection<Object>) entry.getValue(), consumer);
//            }
//            consumer.accept(entry);
//        }
//    }

    private void iterateWithPrefix(String prefix, Map<String, Object> obj, BiFunction<String, Object, Object> filter) {
        for (Map.Entry<String, Object> entry : obj.entrySet()) {
            Object value = entry.getValue();
            if(value instanceof Map) {
                iterateWithPrefix(prefix + entry.getKey() + ".", (Map<String, Object>) value, filter);
            } else if(entry.getValue() instanceof Collection) {
//                iterateListWithPrefix(prefix + entry.getKey() + ".#", (Collection<Object>) entry.getValue(), newList, filter);

                Collection<Object> newList = new ArrayList<>();
                iterateListWithPrefix(prefix + entry.getKey() + ".#", (Collection<Object>) value, newList, filter);
                entry.setValue(newList);
            }
            Object newValue = filter.apply(prefix + entry.getKey(), entry.getValue());
            if(newValue != null) {
                entry.setValue(newValue);
            }
        }
    }

    private void iterateListWithPrefix(String prefix, Collection<Object> collection, Collection<Object> newList, BiFunction<String, Object, Object> filter) {
        int i = 0;
        for (Object entry : collection) {
            Object value = entry;
            if(value instanceof Map) {
                iterateWithPrefix(prefix + i + ".", (Map<String, Object>) value, filter);
            } else if(value instanceof Collection) {
//                iterateListWithPrefix(prefix + i + ".#", (Collection<Object>) entry, newList, filter);
                Collection<Object> newList1 = new ArrayList<>();
                iterateListWithPrefix(prefix + i + ".#", (Collection<Object>) value, newList1, filter);
                value = newList1;
            }
            Object newValue = filter.apply(prefix + i, value);
            if(newValue != null) {
                newList.add(newValue);
            } else {
                newList.add(value);
            }
            i++;
        }
    }

    @Override
    public void iterate(Map<String, Object> map, BiFunction<String, Object, Object> filter) {
        if(map == null || filter == null) {
            return;
        }
        Set<Map.Entry<String, Object>> entrySet = map.entrySet();
        for(Map.Entry<String, Object> entry : entrySet) {
            Object value = entry.getValue();
            if(value instanceof Map) {
                iterateWithPrefix(entry.getKey() + ".", (Map<String, Object>) value, filter);
            } else if(value instanceof Collection) {
                Collection<Object> newList = new ArrayList<>();
                iterateListWithPrefix(entry.getKey() + ".#", (Collection<Object>) value, newList, filter);
                entry.setValue(newList);
            }
            Object newValue = filter.apply(entry.getKey(), entry.getValue());
            if(newValue != null) {
                entry.setValue(newValue);
            }
        }
    }

}
