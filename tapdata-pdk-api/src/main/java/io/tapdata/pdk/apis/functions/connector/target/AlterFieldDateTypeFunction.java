package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.event.ddl.entity.FieldAttrChange;
import io.tapdata.entity.event.ddl.table.TapAlterFieldDataTypeEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;

import java.util.List;

public interface AlterFieldDateTypeFunction {
    void alterFieldDataType(TapConnectorContext connectorContext, TapAlterFieldDataTypeEvent alterFieldDataTypeEvent);
}
