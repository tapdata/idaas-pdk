package io.tapdata.pdk.apis.functions.processor;

import io.tapdata.pdk.apis.context.TapProcessorContext;
import io.tapdata.pdk.apis.functions.consumers.TapProcessConsumer;
import io.tapdata.pdk.apis.entity.TapEvent;

import java.util.List;

/**
 * Process every record
 */
public interface ProcessRecordFunction {
    void process(TapProcessorContext tapProcessorContext, List<TapEvent> recordEvents, TapProcessConsumer<TapEvent> consumer);
}
