package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;

public interface ClearTableFunction {
    void clearTable(TapConnectorContext connectorContext, TapClearTableEvent clearTableEvent) throws Throwable;
}
