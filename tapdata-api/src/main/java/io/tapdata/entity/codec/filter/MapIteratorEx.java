package io.tapdata.entity.codec.filter;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public interface MapIteratorEx {
    void iterate(Map<String, Object> map, BiFunction<String, Object, Object> filter);
}
