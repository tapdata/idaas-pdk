package io.tapdata.pdk.core.workflow.engine;

public class JobOptions {
    protected int queueSize = 20;
    protected int queueBatchSize = 10;

    public int getQueueSize() {
        return queueSize;
    }

    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }

    public int getQueueBatchSize() {
        return queueBatchSize;
    }

    public void setQueueBatchSize(int queueBatchSize) {
        this.queueBatchSize = queueBatchSize;
    }
}
