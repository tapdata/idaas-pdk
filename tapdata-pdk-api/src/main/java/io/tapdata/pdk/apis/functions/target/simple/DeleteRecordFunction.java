package io.tapdata.pdk.apis.functions.target.simple;

import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TapFilter;
import io.tapdata.pdk.apis.functions.consumers.TapWriteListConsumer;

import java.util.List;

public interface DeleteRecordFunction {

    void deleteRecord(TapConnectorContext connectorContext, List<TapFilter> filters, TapWriteListConsumer<TapFilter> deleteConsumer);

}
