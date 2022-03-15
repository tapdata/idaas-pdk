package io.tapdata.pdk.apis.functions.connector.source;

import io.tapdata.pdk.apis.context.TapConnectorContext;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public interface WebHookReadFunction {
    /**
     *
     * @param nodeContext the node context in a DAG
     * @param jsonObjects received record event pushed from webhook
     * @param consumer accept the table and offsetState for the record.
     */
    void read(TapConnectorContext nodeContext, List<Map<String, Object>> jsonObjects, Consumer<List<Map<String, Object>>> consumer) throws Throwable;
}
