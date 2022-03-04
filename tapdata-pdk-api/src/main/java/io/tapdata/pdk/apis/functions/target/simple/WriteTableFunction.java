package io.tapdata.pdk.apis.functions.target.simple;

import io.tapdata.pdk.apis.functions.consumers.TapWriteConsumer;
import io.tapdata.pdk.apis.context.TapProcessorContext;
import io.tapdata.pdk.apis.entity.ddl.TapTableEvent;

//TODO not complete
public interface WriteTableFunction {
    /**
     *
     * @param nodeContext the node context in a DAG
     * @param tableEvent records to create/rename/delete/clear table
     * @param writeConsumer confirm each record write successfully or not
     */
    void writeTable(TapProcessorContext nodeContext, TapTableEvent tableEvent, TapWriteConsumer<TapTableEvent> writeConsumer);
}
