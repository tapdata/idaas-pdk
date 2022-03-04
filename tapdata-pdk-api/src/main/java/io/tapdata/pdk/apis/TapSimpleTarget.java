package io.tapdata.pdk.apis;

import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ddl.TapTableOptions;
import io.tapdata.pdk.apis.functions.SimpleTargetFunctions;
import io.tapdata.pdk.apis.functions.consumers.TapListConsumer;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;

public interface TapSimpleTarget extends TapConnectorNode {
    void init(TapConnectorContext nodeContext, TapNodeSpecification specification);
    void targetFunctions(SimpleTargetFunctions targetFunctions);
}
