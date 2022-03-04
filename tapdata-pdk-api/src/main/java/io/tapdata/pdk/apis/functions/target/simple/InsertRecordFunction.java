package io.tapdata.pdk.apis.functions.target.simple;

import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.consumers.TapWriteListConsumer;
import io.tapdata.pdk.apis.entity.dml.TapRecordEvent;

import java.util.List;

public interface InsertRecordFunction {

    void insertRecord(TapConnectorContext connectorContext, List<TapRecordEvent> recordEvents, TapWriteListConsumer<TapRecordEvent> insertConsumer);

}
