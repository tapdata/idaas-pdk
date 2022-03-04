package io.tapdata.pdk.apis.functions.target;

import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.consumers.TapWriteListConsumer;
import io.tapdata.pdk.apis.entity.dml.TapRecordEvent;

import java.util.List;

public interface DMLFunction {

    /**
     * insert, update, delete events.
     *
     * @param connectorContext
     * @param recordEvents
     * @param writeConsumer
     */
    void writeDML(TapConnectorContext connectorContext, List<TapRecordEvent> recordEvents, TapWriteListConsumer<TapRecordEvent> writeConsumer);

}
