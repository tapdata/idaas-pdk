package io.tapdata.entity.utils.cache;

public interface CacheFactory {
    <T> KVMap<T> getOrCreateKVMap(String mapKey);

    boolean reset(String mapKey);
}
