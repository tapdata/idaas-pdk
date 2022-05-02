package io.tapdata.pdk.apis.functions.connector.source;

import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapFunction;

import java.util.List;

public interface BatchCountFunction extends TapFunction {
    /**
     * @param nodeContext the node context in a DAG
     * @param tableList the tables to count
     * @param offsetState if null, means start from very beginning, otherwise is the start point for batch count.
     *                    type can be any that comfortable for saving offset state.
     */
    long count(TapConnectorContext nodeContext, List<String> tableList, String offsetState) throws Throwable;
}
