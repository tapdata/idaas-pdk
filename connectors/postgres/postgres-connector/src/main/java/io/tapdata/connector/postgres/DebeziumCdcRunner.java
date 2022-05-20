package io.tapdata.connector.postgres;

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
     * start cdc sync
     */
    public void startCdcRunner() {
        engine.run();
    }

    public void stopCdcRunner() {
        if (null != engine && engine.isRunning()) {
            engine.stop();
        }
    }

    public boolean isRunning() {
        return null != engine && engine.isRunning();
    }

    /**
     * close cdc sync
     */
    public void closeCdcRunner() throws IOException {
        engine.close();
        releaseResource();
    }

    protected void releaseResource() {

    }

    public void run() {
        startCdcRunner();
    }

}
