package io.tapdata.pdk.apis;

import io.tapdata.pdk.apis.entity.SupportedTapEvents;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.TargetFunctions;

public interface TapTarget extends TapConnectorNode {
    void init(TapConnectorContext connectorContext, TapNodeSpecification specification);
    void targetFunctions(TargetFunctions targetFunctions, SupportedTapEvents supportedTapEvents);
}
