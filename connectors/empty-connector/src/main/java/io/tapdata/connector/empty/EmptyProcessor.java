package io.tapdata.connector.empty;

import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.apis.TapRecordProcessor;
import io.tapdata.pdk.apis.annotations.TapProcessor;
import io.tapdata.pdk.apis.context.TapContext;
import io.tapdata.pdk.apis.context.TapProcessorContext;
import io.tapdata.pdk.apis.functions.ProcessorFunctions;
import io.tapdata.pdk.apis.functions.consumers.TapProcessConsumer;
import io.tapdata.pdk.apis.functions.processor.ProcessRecordFunction;
import io.tapdata.pdk.apis.entity.TapEvent;

import java.util.List;

@TapProcessor("processor.json")
public class EmptyProcessor implements TapRecordProcessor {
    @Override
    public void destroy() {

    }

    @Override
    public void init(TapContext tapContext, TapNodeSpecification specification) {

    }

    @Override
    public void processorFunctions(ProcessorFunctions processorFunctions) {
        processorFunctions.withProcessRecordFunction(new ProcessRecordFunction() {
            @Override
            public void process(TapProcessorContext tapContext, List<TapEvent> recordEvents, TapProcessConsumer<TapEvent> consumer) {
                consumer.accept(recordEvents, null);
            }
        });
    }
}
