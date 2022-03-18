package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.util.List;
import java.util.function.Consumer;

public interface WriteRecordFunction {

    /**
     * insert, update, delete events.
     *
     * @param connectorContext
     * @param recordEvents
     * @param consumer
     */
    void writeDML(TapConnectorContext connectorContext, List<TapRecordEvent> recordEvents, Consumer<WriteListResult<TapRecordEvent>> consumer) throws Throwable;

}
