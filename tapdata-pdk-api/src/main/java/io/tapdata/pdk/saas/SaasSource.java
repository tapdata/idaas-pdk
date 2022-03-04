package io.tapdata.pdk.saas;

import io.tapdata.pdk.apis.TapSource;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ddl.TapTableOptions;
import io.tapdata.pdk.apis.functions.SourceFunctions;
import io.tapdata.pdk.apis.functions.consumers.TapListConsumer;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;

import java.util.function.Consumer;

public interface SaasSource extends TapSource {
    @Override
    void init(TapConnectorContext nodeContext, TapNodeSpecification specification);
    @Override
    void discoverSchema(TapConnectionContext databaseContext, TapListConsumer<TapTableOptions> tapListConsumer);
    @Override
    void sourceFunctions(SourceFunctions sourceFunctions);
}
