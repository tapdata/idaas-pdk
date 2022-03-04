package io.tapdata.pdk.apis.functions.target;

import io.tapdata.pdk.apis.context.TapProcessorContext;
import io.tapdata.pdk.apis.functions.consumers.TapWriteListConsumer;
import io.tapdata.pdk.apis.entity.dml.TapRecordEvent;

import java.util.List;

public interface TransactionFunction {

    /**
     * insert, update, delete events.
     *
     * @param nodeContext
     * @param recordEvents
     * @param writeConsumer
     */
    void executeTransaction(TapProcessorContext nodeContext, List<TapRecordEvent> recordEvents, TapWriteListConsumer<TapRecordEvent> writeConsumer);

}
