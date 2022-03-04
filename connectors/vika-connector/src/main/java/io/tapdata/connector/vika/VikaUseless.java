package io.tapdata.connector.vika;

import io.tapdata.pdk.apis.TapSource;
import io.tapdata.pdk.apis.annotations.TapConnector;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionTestResult;
import io.tapdata.pdk.apis.entity.ddl.TapTableOptions;
import io.tapdata.pdk.apis.functions.SourceFunctions;
import io.tapdata.pdk.apis.functions.consumers.TapListConsumer;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;

import java.util.function.Consumer;

@TapConnector("target1.json")
public class VikaUseless implements TapSource {
    @Override
    public void discoverSchema(TapConnectionContext databaseContext, TapListConsumer<TapTableOptions> tapReadOffsetConsumer) {

    }

    @Override
    public ConnectionTestResult connectionTest(TapConnectionContext databaseContext) {
        return null;
    }

    @Override
    public void destroy() {

    }

    @Override
    public void init(TapConnectorContext nodeContext, TapNodeSpecification specification) {

    }

    @Override
    public void sourceFunctions(SourceFunctions sourceFunctions) {

    }
}
