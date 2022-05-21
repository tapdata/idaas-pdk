package io.tapdata.pdk.apis.functions.connector.source;

import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapFunction;

import java.util.List;
import java.util.function.BiConsumer;

public interface StreamOffsetFunction extends TapFunction {
    /**
     *
     * @param connectorContext the node context in a DAG
     * @param offsetStartTime specify the expected start time to return the offset. If null, return current offset.
     * @return
     */
    void streamOffset(TapConnectorContext connectorContext, List<String> tableList, Long offsetStartTime, BiConsumer<Object, Long> offsetOffsetTimeConsumer) throws Throwable;
}
