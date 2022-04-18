package io.tapdata.pdk.apis.functions.connector.source;

import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapFunction;

public interface BatchCountFunction extends TapFunction {
    /**
     * @param nodeContext the node context in a DAG
     * @param offsetState if null, means start from very beginning, otherwise is the start point for batch count.
     *                    type can be any that comfortable for saving offset state.
     */
    long count(TapConnectorContext nodeContext, String offsetState) throws Throwable;
}
