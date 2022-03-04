package io.tapdata.pdk.apis.functions.target.simple;

import io.tapdata.pdk.apis.entity.TapFilter;
import io.tapdata.pdk.apis.entity.dml.TapRecordEvent;
import io.tapdata.pdk.apis.context.TapProcessorContext;
import io.tapdata.pdk.apis.functions.consumers.TapWriteListConsumer;

import java.util.List;

/**
 * If upsert record function not implemented, will use query by filter function first, update if exists or insert if not.
 */
public interface UpsertRecordFunction {

    void upsertRecord(TapProcessorContext nodeContext, List<TapFilter> filters, List<TapRecordEvent> values, TapWriteListConsumer<TapFilter> upsertConsumer);

}
