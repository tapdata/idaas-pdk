package io.tapdata.pdk.apis.functions.target;

import io.tapdata.pdk.apis.entity.TapFilter;
import io.tapdata.pdk.apis.entity.dml.TapRecordEvent;
import io.tapdata.pdk.apis.context.TapProcessorContext;
import io.tapdata.pdk.apis.functions.consumers.TapListConsumer;

import java.util.List;

/**
 * Will be used when upsert function not implemented. for query update/insert model
 *
 */
public interface QueryByFilterFunction {
    void query(TapProcessorContext nodeContext, List<TapFilter> filters, TapListConsumer<TapRecordEvent> queryConsumer);
}
