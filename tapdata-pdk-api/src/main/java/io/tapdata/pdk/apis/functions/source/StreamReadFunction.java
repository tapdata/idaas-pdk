package io.tapdata.pdk.apis.functions.source;

import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.consumers.TapReadOffsetConsumer;
import io.tapdata.pdk.apis.entity.TapEvent;

public interface StreamReadFunction {
    /**
     *
     * @param nodeContext the node context in a DAG
     * @param offsetState if null, means start from very beginning, otherwise is the start point for batch reading.
     *                    type can be any that comfortable for saving offset state.
     * @param tapReadOffsetConsumer accept the table and offsetState for the record.
     */
    void streamRead(TapConnectorContext nodeContext, Object offsetState, TapReadOffsetConsumer<TapEvent> tapReadOffsetConsumer);
}
