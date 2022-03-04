package io.tapdata.pdk.apis.functions.target.simple;

import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.consumers.TapWriteConsumer;
import io.tapdata.pdk.apis.entity.ddl.TapSchemaEvent;

//TODO not complete
public interface WriteSchemaFunction {
    /**
     *
     * @param nodeContext the node context in a DAG
     * @param schemaEvent schema change event
     * @param writeConsumer confirm each record write successfully or not
     */
    void writeSchema(TapConnectorContext nodeContext, TapSchemaEvent schemaEvent, TapWriteConsumer<TapSchemaEvent> writeConsumer);
}
