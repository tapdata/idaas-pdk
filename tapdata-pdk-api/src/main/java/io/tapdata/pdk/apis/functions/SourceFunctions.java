package io.tapdata.pdk.apis.functions;

import io.tapdata.pdk.apis.functions.source.*;

public class SourceFunctions extends CommonFunctions<SourceFunctions> {
    private BatchReadFunction batchReadFunction;
    private StreamReadFunction streamReadFunction;
    private WebHookReadFunction webHookReadFunction;
    private BatchCountFunction batchCountFunction;
    private BatchOffsetFunction batchOffsetFunction;
    private StreamOffsetFunction streamOffsetFunction;

    /**
     * TODO Not integrated yet.
     *
     * @param function
     * @return
     */
    public SourceFunctions withBatchOffsetFunction(BatchOffsetFunction function) {
        batchOffsetFunction = function;
        return this;
    }

    /**
     * TODO Not integrated yet.
     *
     * @param function
     * @return
     */
    public SourceFunctions withStreamOffsetFunction(StreamOffsetFunction function) {
        streamOffsetFunction = function;
        return this;
    }

    public SourceFunctions withBatchReadFunction(BatchReadFunction function) {
        batchReadFunction = function;
        return this;
    }

    public SourceFunctions withStreamReadFunction(StreamReadFunction function) {
        streamReadFunction = function;
        return this;
    }

    public SourceFunctions withWebHookReadFunction(WebHookReadFunction function) {
        webHookReadFunction = function;
        return this;
    }

    public SourceFunctions withBatchCountFunction(BatchCountFunction function) {
        this.batchCountFunction = function;
        return this;
    }

    public BatchReadFunction getBatchReadFunction() {
        return batchReadFunction;
    }

    public StreamReadFunction getStreamReadFunction() {
        return streamReadFunction;
    }

    public WebHookReadFunction getWebHookReadFunction() {
        return webHookReadFunction;
    }

    public BatchCountFunction getBatchCountFunction() {
        return batchCountFunction;
    }

    public BatchOffsetFunction getBatchOffsetFunction() {
        return batchOffsetFunction;
    }

    public StreamOffsetFunction getStreamOffsetFunction() {
        return streamOffsetFunction;
    }
}
