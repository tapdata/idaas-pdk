package io.tapdata.pdk.saas;

import io.tapdata.pdk.apis.TapTarget;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.SupportedTapEvents;
import io.tapdata.pdk.apis.entity.ddl.TapTableOptions;
import io.tapdata.pdk.apis.functions.TargetFunctions;
import io.tapdata.pdk.apis.functions.consumers.TapListConsumer;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;

import java.util.function.Consumer;

public interface SaasTarget extends TapTarget {
    @Override
    void init(TapConnectorContext connectorContext, TapNodeSpecification specification);

    @Override
    void discoverSchema(TapConnectionContext databaseContext, TapListConsumer<TapTableOptions> tapReadOffsetConsumer);

    @Override
    void targetFunctions(TargetFunctions targetFunctions, SupportedTapEvents supportedTapEvents);
}
