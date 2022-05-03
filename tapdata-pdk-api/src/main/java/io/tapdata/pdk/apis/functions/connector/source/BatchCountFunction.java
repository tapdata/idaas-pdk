package io.tapdata.pdk.apis.functions.connector.source;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapFunction;

import java.util.List;

public interface BatchCountFunction extends TapFunction {
    /**
     * @param nodeContext the node context in a DAG
     * @param table the table to count
     * @param offsetState if null, means start from very beginning, otherwise is the start point for batch count.
     *                    type can be any that comfortable for saving offset state.
     */
    long count(TapConnectorContext nodeContext, TapTable table, String offsetState) throws Throwable;
}
