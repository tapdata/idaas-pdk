package io.tapdata.pdk.core.api;

import io.tapdata.pdk.apis.TapRecordProcessor;
import io.tapdata.pdk.apis.context.TapContext;
import io.tapdata.pdk.apis.context.TapProcessorContext;
import io.tapdata.pdk.apis.functions.ProcessorFunctions;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;

public class ProcessorNode extends Node {
    TapRecordProcessor processor;
    TapProcessorContext processorContext;

    ProcessorFunctions processorFunctions;

    void processorFunctions(ProcessorFunctions processorFunctions) {
        processor.processorFunctions(processorFunctions);
    }

    public void init(TapNodeSpecification specification) {
        processor.init(processorContext, specification);
    }

    public TapProcessorContext getProcessorContext() {
        return processorContext;
    }

    public TapRecordProcessor getProcessor() {
        return processor;
    }

    public ProcessorFunctions getProcessorFunctions() {
        return processorFunctions;
    }

}
