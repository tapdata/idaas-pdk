package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.event.ddl.table.TapAlterTableEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapFunction;

public interface AlterTableFunction extends TapFunction {
    void alterTable(TapConnectorContext connectorContext, TapAlterTableEvent alterTableEvent) throws Throwable;
}
