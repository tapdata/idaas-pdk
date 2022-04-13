package io.tapdata.pdk.apis.functions.connector.source;

import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapFunction;

public interface StreamOffsetFunction extends TapFunction {
    /**
     *
     * @param offsetStartTime specify the expected start time to return the offset. If null, return current offset.
     * @param connectorContext the node context in a DAG
     */
    String streamOffset(TapConnectorContext connectorContext, Long offsetStartTime) throws Throwable;
}
