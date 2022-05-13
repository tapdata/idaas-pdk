package io.tapdata.postgres;

import io.debezium.embedded.EmbeddedEngine;
import io.debezium.engine.DebeziumEngine;
import io.tapdata.postgres.config.PostgresDebeziumConfig;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.util.List;

public abstract class DebeziumCdcRunner implements Runnable {

    protected PostgresDebeziumConfig postgresDebeziumConfig;
    protected EmbeddedEngine engine;
    protected String runnerName;

    protected DebeziumCdcRunner() {

    }

    public String getRunnerName() {
        return runnerName;
    }

    public void consumeRecords(List<SourceRecord> sourceRecords, DebeziumEngine.RecordCommitter<SourceRecord> committer) {

    }

    public void startCdcRunner() {
        if (engine != null && !engine.isRunning()) {
            new Thread(() -> engine.run()).start();
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

    public void closeCdcRunner() {
        new Thread(() -> {
            try {
                engine.close();
                releaseResource();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).start();
    }

    protected void releaseResource() {

    }

    public void run() {
        startCdcRunner();
    }

}
