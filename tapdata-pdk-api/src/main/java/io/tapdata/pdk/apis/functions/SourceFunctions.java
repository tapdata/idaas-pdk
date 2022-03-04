package io.tapdata.pdk.apis.functions;

import io.tapdata.pdk.apis.functions.source.BatchCountFunction;
import io.tapdata.pdk.apis.functions.source.BatchReadFunction;
import io.tapdata.pdk.apis.functions.source.StreamReadFunction;
import io.tapdata.pdk.apis.functions.source.WebHookReadFunction;

public class SourceFunctions extends CommonFunctions<SourceFunctions> {
    private BatchReadFunction batchReadFunction;
    private StreamReadFunction streamReadFunction;
    private WebHookReadFunction webHookReadFunction;
    private BatchCountFunction batchCountFunction;

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
}
