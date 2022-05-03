package io.tapdata.entity.utils.cache;

public interface KVMapFactory {
    <T> KVMap<T> getCacheMap(String mapKey);

    <T> KVMap<T> getPersistentMap(String mapKey);

    <T> KVReadOnlyMap<T> createKVReadOnlyMap(String mapKey);

    boolean reset(String mapKey);
}
