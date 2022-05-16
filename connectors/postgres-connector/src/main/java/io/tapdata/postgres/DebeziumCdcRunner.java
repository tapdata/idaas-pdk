package io.tapdata.postgres;

import io.debezium.embedded.EmbeddedEngine;
import io.debezium.engine.DebeziumEngine;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.util.List;

/**
 * Abstract runner for change data capture
 *
 * @author Jarad
 * @date 2022/5/13
 */
public abstract class DebeziumCdcRunner implements Runnable {

    protected EmbeddedEngine engine;
    protected String runnerName;

    protected DebeziumCdcRunner() {

    }

    public String getRunnerName() {
        return runnerName;
    }

    /**
     * records caught by cdc can only be consumed in this method
     */
    public void consumeRecords(List<SourceRecord> sourceRecords, DebeziumEngine.RecordCommitter<SourceRecord> committer) {

    }

    /**
     * start cdc async
     */
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

    /**
     * close cdc async
     */
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
