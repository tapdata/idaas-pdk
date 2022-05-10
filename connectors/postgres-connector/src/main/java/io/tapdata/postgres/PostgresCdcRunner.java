package io.tapdata.postgres;

import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.postgres.config.DebeziumConfig;
import io.tapdata.postgres.config.PostgresConfig;

import java.io.IOException;
import java.util.List;

public class PostgresCdcRunner implements Runnable {

    private final Configuration configuration;
    private EmbeddedEngine engine;

    public PostgresCdcRunner(PostgresConfig postgresConfig) {
        this.configuration = new DebeziumConfig(postgresConfig).create();
    }

    public PostgresCdcRunner watch(List<String> observedTableList) {
        return this;
    }

    public PostgresCdcRunner connect(PostgresConnector postgresConnector, Object offsetState, int recordSize, StreamReadConsumer consumer) {
        this.engine = EmbeddedEngine.create()
                .using(configuration)
                .notifying(sourceRecord -> postgresConnector.consumeRecord(sourceRecord, offsetState, recordSize, consumer))
                .build();
        return this;
    }

    @Override
    public void run() {
        engine.run();
    }

    public void stopCdcRunner() {
        if (engine != null && engine.isRunning()) {
            engine.stop();
        }
    }

    public boolean isRunning() {
        return engine != null && engine.isRunning();
    }

    public void closeCdc() throws IOException {
        engine.close();
    }

}
