package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;

public interface DropTableFunction {
    void dropTable(TapConnectorContext connectorContext, TapDropTableEvent dropTableEvent) throws Throwable;
}
