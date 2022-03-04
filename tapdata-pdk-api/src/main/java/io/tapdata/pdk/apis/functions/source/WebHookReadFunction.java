package io.tapdata.pdk.apis.functions.source;

import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TapEvent;
import io.tapdata.pdk.apis.functions.consumers.TapReadConsumer;

import java.util.List;
import java.util.Map;

public interface WebHookReadFunction {
    /**
     *
     * @param nodeContext the node context in a DAG
     * @param jsonObjects received record event pushed from webhook
     * @param tapReadConsumer accept the table and offsetState for the record.
     */
    void read(TapConnectorContext nodeContext, List<Map<String, Object>> jsonObjects, TapReadConsumer<TapEvent> tapReadConsumer);
}
