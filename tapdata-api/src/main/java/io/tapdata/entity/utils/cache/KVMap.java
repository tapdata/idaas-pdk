package io.tapdata.entity.utils.cache;


public interface KVMap<T> {
    void init(String mapKey);
    T put(String key, T t);
    T putIfAbsent(String key, T t);

    T get(String key);
    T remove(String key);
    void clear();
    void reset();
}
