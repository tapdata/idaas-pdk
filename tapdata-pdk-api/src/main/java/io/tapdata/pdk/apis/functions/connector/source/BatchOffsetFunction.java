package io.tapdata.pdk.apis.functions.connector.source;

import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapFunction;

public interface BatchOffsetFunction extends TapFunction {
    /**
     *
     * @param connectorContext the node context in a DAG
     */
    String batchOffset(TapConnectorContext connectorContext) throws Throwable;
}
