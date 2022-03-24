package io.tapdata.pdk.core.workflow.engine;

import java.util.List;

public class JobOptions {
    protected int queueSize = 20;
    protected int queueBatchSize = 10;
    public static final String ACTION_DROP_TABLE = "dropTable";
    public static final String ACTION_CREATE_TABLE = "createTable";
    public static final String ACTION_CLEAR_TABLE = "clearTable";
    public static final String ACTION_INDEX_PRIMARY = "indexPrimary";
    protected List<String> actionsBeforeStart;

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

    public List<String> getActionsBeforeStart() {
        return actionsBeforeStart;
    }

    public void setActionsBeforeStart(List<String> actionsBeforeStart) {
        this.actionsBeforeStart = actionsBeforeStart;
    }
}
