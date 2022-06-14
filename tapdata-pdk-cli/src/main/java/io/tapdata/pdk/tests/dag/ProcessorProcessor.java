package io.tapdata.pdk.tests.dag;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Watermark;
import io.tapdata.entity.logger.TapLogger;

import javax.annotation.Nonnull;

public class ProcessorProcessor extends AbstractProcessor {
    private static final String TAG = ProcessorProcessor.class.getSimpleName();

    public boolean isCooperative() {
        return false;
    }
    public void init(@Nonnull Context context) throws Exception {
        TapLogger.info(TAG, "init {}", context);
    }
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        TapLogger.info(TAG, "tryProcess ordinal {} item {}", ordinal, item);
//        throw new UnsupportedOperationException("Missing implementation in " + getClass());
        return true;
    }
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        TapLogger.info(TAG, "restoreFromSnapshot key {} value {}", key, value);
//        throw new UnsupportedOperationException("Missing implementation in " + getClass());
    }
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        TapLogger.info(TAG, "tryProcessWatermark {}", watermark);
        return tryEmit(watermark);
    }
    public boolean complete() {
        TapLogger.info(TAG, "complete");
        return true;
    }
    public boolean saveToSnapshot() {
        TapLogger.info(TAG, "saveToSnapshot");
        return true;
    }
    public boolean snapshotCommitPrepare() {
        TapLogger.info(TAG, "snapshotCommitPrepare");
        return true;
    }
    public boolean snapshotCommitFinish(boolean success) {
        TapLogger.info(TAG, "snapshotCommitPrepare success {}", success);
        return true;
    }
    public boolean finishSnapshotRestore() {
        TapLogger.info(TAG, "finishSnapshotRestore");
        return true;
    }

    public void close() throws Exception {
        TapLogger.info(TAG, "close");
    }
    public boolean closeIsCooperative() {
        TapLogger.info(TAG, "closeIsCooperative");
        return false;
    }
}