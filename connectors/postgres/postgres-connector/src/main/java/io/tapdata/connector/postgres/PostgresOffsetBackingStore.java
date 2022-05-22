package io.tapdata.connector.postgres;

import com.alibaba.fastjson.JSONObject;
import io.tapdata.connector.postgres.kit.EmptyKit;
import io.tapdata.connector.postgres.storage.PostgresOffsetStorage;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class PostgresOffsetBackingStore extends MemoryOffsetBackingStore {

    private PostgresOffset postgresOffset;
    private String slotName;

    public PostgresOffsetBackingStore() {
    }

    public void configure(WorkerConfig config) {
        super.configure(config);
        this.slotName = (String) config.originals().get("slot.name");
        this.postgresOffset = PostgresOffsetStorage.postgresOffsetMap.get(slotName);
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
            if (EmptyKit.isNotNull(key)) {
                postgresOffset.setStreamOffsetKey(new String(key.array()));
                postgresOffset.setStreamOffsetValue(new String(value.array()));
                JSONObject jsonObject = JSONObject.parseObject(postgresOffset.getStreamOffsetValue());
                postgresOffset.setStreamOffsetTime(jsonObject.getLong("ts_usec"));
            }
        });
        PostgresOffsetStorage.postgresOffsetMap.put(slotName, postgresOffset);
        PostgresOffsetStorage.manyOffsetMap.get(slotName).add(postgresOffset);
    }

}
