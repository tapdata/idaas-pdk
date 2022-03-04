package io.tapdata.pdk.core.api;

import io.tapdata.pdk.apis.TapTarget;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.SupportedTapEvents;
import io.tapdata.pdk.apis.functions.TargetFunctions;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;

public class TargetNode extends Node {
    TapTarget target;
    TapConnectorContext connectorContext;

    TargetFunctions targetFunctions;

    SupportedTapEvents supportedTapEvents;

    public void init(TapNodeSpecification specification) {
        target.init(connectorContext, specification);
    }
    public void targetFunctions(TargetFunctions targetFunctions) {
        target.targetFunctions(targetFunctions, supportedTapEvents);
    }

    public TapConnectorContext getConnectorContext() {
        return connectorContext;
    }

    public TapTarget getTarget() {
        return target;
    }

    public TargetFunctions getTargetFunctions() {
        return targetFunctions;
    }

    public SupportedTapEvents getSupportedTapEvents() {
        return supportedTapEvents;
    }
}
