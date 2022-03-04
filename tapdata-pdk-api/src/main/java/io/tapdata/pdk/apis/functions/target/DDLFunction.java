package io.tapdata.pdk.apis.functions.target;

import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.consumers.TapWriteConsumer;
import io.tapdata.pdk.apis.entity.ddl.TapDDLEvent;

public interface DDLFunction {

    /**
     * insert, update, delete events.
     *
     *
     * @param connectorContext
     * @param writeConsumer
     */
    void writeDDL(TapConnectorContext connectorContext, TapDDLEvent ddlEvent, TapWriteConsumer<TapDDLEvent> writeConsumer);

}
