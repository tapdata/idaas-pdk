package io.tapdata.entity.utils.cache;

public interface CacheFactory {
    <T> KVMap<T> getOrCreateKVMap(String mapKey);

    <T> KVReadOnlyMap<T> createKVReadOnlyMap(String mapKey);

    boolean reset(String mapKey);
}
