package io.tapdata.connector.postgres;

import io.tapdata.connector.postgres.kit.EmptyKit;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PostgresOffsetBackingStore extends MemoryOffsetBackingStore {

    private PostgresOffset postgresOffset;
    private String slotName;

    public PostgresOffsetBackingStore() {
    }

    public void configure(WorkerConfig config) {
        super.configure(config);
        this.slotName = (String) config.originals().get("slot.name");
        this.postgresOffset = postgresOffsetMap.get(slotName);
    }

    public synchronized void start() {
        super.start();
        this.load();
    }

    public synchronized void stop() {
        super.stop();
    }

    private void load() {
        if (EmptyKit.isNull(postgresOffset) || EmptyKit.isNull(postgresOffset.getStreamOffsetKey())) {
            this.data = new HashMap<>();
        } else {
            this.data.put(ByteBuffer.wrap(postgresOffset.getStreamOffsetKey().getBytes()), ByteBuffer.wrap(postgresOffset.getStreamOffsetValue().getBytes()));
        }
    }

    protected void save() {
        Map<byte[], byte[]> raw = new HashMap<>();
        this.data.forEach((key, value) -> {
            postgresOffset.setStreamOffsetKey(EmptyKit.isNotNull(key) ? new String(key.array()) : null);
            postgresOffset.setStreamOffsetValue(EmptyKit.isNotNull(value) ? new String(value.array()) : null);
        });
        PostgresOffsetBackingStore.postgresOffsetMap.put(slotName, postgresOffset);
        System.out.println(postgresOffset.getStreamOffsetKey());
        System.out.println(postgresOffset.getStreamOffsetValue());
    }

    static Map<String, PostgresOffset> postgresOffsetMap = new ConcurrentHashMap<>();

}
