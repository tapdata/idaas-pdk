package io.tapdata.pdk.core.utils.cache;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVMap;
import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.SerializerException;

import java.io.File;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;

import static io.tapdata.entity.simplify.TapSimplify.table;

@Implementation(value = KVMap.class, buildNumber = 0, type = "ehcache")
public class EhcacheKVMap<T> implements KVMap<T> {
    private PersistentCacheManager persistentCacheManager;
    private Cache<String, T> cache;
    private Class<T> tClass;
    @Override
    public void init(String mapKey) {
        Class<?> theClass = null;
        Type type = this.getClass().getGenericSuperclass();
        if(type instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) type;
            Type[] types = parameterizedType.getActualTypeArguments();
            if(types != null && types.length == 1) {
                theClass = (Class<?>) types[0];
            }
        }

        persistentCacheManager = CacheManagerBuilder.newCacheManagerBuilder()
                .with(CacheManagerBuilder.persistence(new File(getStoragePath(), "myData")))
                .withCache(mapKey,
                        CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, theClass,
                                ResourcePoolsBuilder.newResourcePoolsBuilder()
                                        .heap(10, EntryUnit.ENTRIES)
                                        .offheap(1, MemoryUnit.MB)
                                        .disk(20, MemoryUnit.MB, true)
                        )
                ).build(true);


        cache = (Cache<String, T>) persistentCacheManager.getCache(mapKey, String.class, theClass);

    }

    public static void main(String... args) {
        EhcacheKVMap<TapTable> tableMap = new EhcacheKVMap<>();
        tableMap.init("AAAAA");

        tableMap.cache.put("a", table("name"));
        TapTable table = tableMap.cache.get("a");
    }
    private File getStoragePath() {
        return new File("./");
    }

    @Override
    public T put(String key, T t) {
        return null;
    }

    @Override
    public T putIfAbsent(String key, T t) {
        return null;
    }

    @Override
    public T get(String key) {
        return null;
    }

    @Override
    public T remove(String key) {
        return null;
    }

    @Override
    public void clear() {

    }

    @Override
    public void reset() {

    }
}
