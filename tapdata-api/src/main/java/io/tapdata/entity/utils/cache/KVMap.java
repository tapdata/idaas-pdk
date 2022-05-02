package io.tapdata.entity.utils.cache;


public interface KVMap<T> extends KVReadOnlyMap<T> {
    void init(String mapKey);
    T put(String key, T t);
    T putIfAbsent(String key, T t);

    T remove(String key);
    void clear();
    void reset();
}
