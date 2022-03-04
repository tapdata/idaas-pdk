package io.tapdata.pdk.apis.functions.target.simple;

import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TapFilter;
import io.tapdata.pdk.apis.entity.dml.TapRecordEvent;
import io.tapdata.pdk.apis.functions.consumers.TapWriteListConsumer;

import java.util.List;

public interface UpdateRecordFunction {

    void updateRecord(TapConnectorContext nodeContext, List<TapFilter> filters, List<TapRecordEvent> values, TapWriteListConsumer<TapFilter> updateConsumer);

}
