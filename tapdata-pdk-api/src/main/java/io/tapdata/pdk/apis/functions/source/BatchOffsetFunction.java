package io.tapdata.pdk.apis.functions.source;

import io.tapdata.pdk.apis.context.TapConnectorContext;

public interface BatchOffsetFunction {
    /**
     *
     * @param connectorContext the node context in a DAG
     */
    Object batchOffset(TapConnectorContext connectorContext);
}
