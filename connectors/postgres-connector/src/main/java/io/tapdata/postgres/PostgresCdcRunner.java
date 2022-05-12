package io.tapdata.postgres;

import io.debezium.embedded.EmbeddedEngine;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.postgres.config.DebeziumConfig;
import io.tapdata.postgres.config.PostgresConfig;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;


public class PostgresCdcRunner implements Runnable {

    private final PostgresConfig postgresConfig;
    private final DebeziumConfig debeziumConfig;
    private EmbeddedEngine engine;
    private final String slotName;
    private Object offsetState;
    private int recordSize;
    private StreamReadConsumer consumer;

    public PostgresCdcRunner(PostgresConfig postgresConfig, List<String> observedTableList) {
        this.postgresConfig = postgresConfig;
        debeziumConfig = new DebeziumConfig(postgresConfig, observedTableList);
        this.slotName = debeziumConfig.getSlotName();
    }

    public PostgresCdcRunner consumeOffset(Object offsetState, int recordSize, StreamReadConsumer consumer) {
        this.engine = EmbeddedEngine.create()
                .using(debeziumConfig.create())
                .notifying(this::consumeRecord)
                .build();
        this.offsetState = offsetState;
        this.recordSize = recordSize;
        this.consumer = consumer;
        return this;
    }

    public void consumeRecord(SourceRecord sourceRecord) {
        System.out.println(sourceRecord);
        System.out.println(offsetState);
        System.out.println(recordSize);
        PostgresCdcPool.removeRunner(slotName);
//        TapSimplify.sleep(10000);
//        consumer.accept(TapSimplify.list());
    }

    public String getSlotName() {
        return slotName;
    }

    public void startCdcRunner() {
        if (engine != null && !engine.isRunning()) {
            new Thread(() -> {
                engine.run();
            }).start();
        }
    }

    public void stopCdcRunner() {
        if (engine != null && engine.isRunning()) {
            engine.stop();
        }
    }

    public boolean isRunning() {
        return engine != null && engine.isRunning();
    }

    public void closeCdc() {
        new Thread(() -> {
            try {
                engine.close();
                clearSlot();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).start();
    }

    public void clearSlot() {
        try {
            String dbUrl = postgresConfig.getDatabaseUrl();
            Class.forName(postgresConfig.getJdbcDriver());
            Connection conn = DriverManager.getConnection(dbUrl, postgresConfig.getUser(), postgresConfig.getPassword());
            conn.createStatement().execute("SELECT PG_DROP_REPLICATION_SLOT('" + slotName + "')");
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        startCdcRunner();
    }
}
