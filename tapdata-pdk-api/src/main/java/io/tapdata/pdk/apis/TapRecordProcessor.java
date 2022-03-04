package io.tapdata.pdk.apis;

import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.apis.context.TapContext;
import io.tapdata.pdk.apis.functions.ProcessorFunctions;

public interface TapRecordProcessor extends TapNode {
    void init(TapContext tapContext, TapNodeSpecification specification);
    void processorFunctions(ProcessorFunctions processorFunctions);
}
