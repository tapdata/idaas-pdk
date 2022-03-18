package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.event.ddl.table.TapAlterTableEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;

public interface CreateTableFunction {
    void createTable(TapConnectorContext connectorContext, TapCreateTableEvent createTableEvent) throws Throwable;
}
