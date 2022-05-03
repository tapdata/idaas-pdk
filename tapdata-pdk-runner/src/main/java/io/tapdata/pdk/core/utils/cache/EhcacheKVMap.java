package io.tapdata.pdk.core.utils.cache;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.Status;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;

import java.io.File;

import static io.tapdata.entity.simplify.TapSimplify.field;
import static io.tapdata.entity.simplify.TapSimplify.table;

@Implementation(value = KVMap.class, buildNumber = 0, type = "ehcache")
public class EhcacheKVMap<T> implements KVMap<T> {
    private static final String TAG = EhcacheKVMap.class.getSimpleName();
    private PersistentCacheManager persistentCacheManager = null;
    private Cache<String, T> cache;
    private String cacheKey;
    @Override
    public void init(String mapKey, Class<T> valueClass) {
        if(persistentCacheManager == null) {
            synchronized (this) {
                if(persistentCacheManager == null) {
                    cacheKey = mapKey;

                    String cacheFolder = CommonUtils.getProperty("tapcache.ehcache_root_path", "cacheData");
                    int maxHeapEntries = CommonUtils.getPropertyInt("tapcache_ehcache_heap_max_entries", 10);
                    int maxDiskSize = CommonUtils.getPropertyInt("tapcache_ehcache_disk_max_mb", 512);

                    PersistentCacheManager persistentCacheManager = CacheManagerBuilder.newCacheManagerBuilder()
                            .with(CacheManagerBuilder.persistence(new File(getStoragePath(), cacheFolder)))
                            .withCache(mapKey,
                                    CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, (Class<?>) valueClass,
                                            ResourcePoolsBuilder.newResourcePoolsBuilder()
                                                    .heap(maxHeapEntries, EntryUnit.ENTRIES)
//                                                  .offheap(20, MemoryUnit.MB)
                                                    .disk(maxDiskSize, MemoryUnit.MB, true)
                                    )
                            )
                            .build(true);
                    this.persistentCacheManager = persistentCacheManager;
                    //noinspection unchecked
                    cache = (Cache<String, T>) this.persistentCacheManager.getCache(mapKey, String.class, (Class<?>) valueClass);
                }
            }
        }

    }

    public static void main(String... args) {
        EhcacheKVMap<TapTable> tableMap = new EhcacheKVMap<>();
        tableMap.init("AAAAA", TapTable.class);

        tableMap.put("a", table("name").add(field("field", "TapString").comment("asdkfalskdflskdfj")));

        long putTime = System.currentTimeMillis();
        for(int i = 0; i < 1; i++) {
            String key = String.valueOf(i);
            tableMap.put(key, table(key).add(field("field", "TapString").comment("asdkfalskdflskdfjasdkfalskdflskdfjasdkfalskdflskdfjasdkfalskdflskdfjasdkfalskdflskdfjasdkfalskdflskdfjasdkfalskdflskdfjasdkfalskdflskdfjasdkfalskdflskdfj")));
        }
        System.out.println("put takes " + (System.currentTimeMillis() - putTime));

        long getTime = System.currentTimeMillis();
        for(int i = 0; i < 1000000; i++) {
            TapTable table = tableMap.get("a");
        }
        System.out.println("get take " + (System.currentTimeMillis() - getTime));

        System.out.println(tableMap.get("a"));

        tableMap.clear();
        tableMap.reset();
        tableMap.reset();
    }
    private File getStoragePath() {
        return new File("./");
    }

    @Override
    public void put(String key, T t) {
        cache.put(key, t);
    }

    @Override
    public T putIfAbsent(String key, T t) {
        return cache.putIfAbsent(key, t);
    }

    @Override
    public T get(String key) {
        return cache.get(key);
    }

    @Override
    public T remove(String key) {
        T value = cache.get(key);
        if(value != null) {
            cache.remove(key);
            return value;
        }
        return null;
    }

    @Override
    public void clear() {
        cache.clear();
    }

    @Override
    public void reset() {
        if(!persistentCacheManager.getStatus().equals(Status.UNINITIALIZED)) {
            CommonUtils.ignoreAnyError(() -> persistentCacheManager.destroyCache(cacheKey), TAG);
            persistentCacheManager.close();
        }
    }
}
