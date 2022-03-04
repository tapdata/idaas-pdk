package io.tapdata.connector.empty;

import io.tapdata.pdk.apis.TapSource;
import io.tapdata.pdk.apis.TapTarget;
import io.tapdata.pdk.apis.annotations.TapConnector;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionTestResult;
import io.tapdata.pdk.apis.entity.SupportedTapEvents;
import io.tapdata.pdk.apis.entity.ddl.TapTable;
import io.tapdata.pdk.apis.entity.ddl.TapTableOptions;
import io.tapdata.pdk.apis.functions.SourceFunctions;
import io.tapdata.pdk.apis.functions.TargetFunctions;
import io.tapdata.pdk.apis.functions.consumers.TapListConsumer;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;

import java.util.*;
import java.util.function.Consumer;

@TapConnector("allsource.json")
public class allSource implements TapSource, TapTarget {

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
    public void targetFunctions(TargetFunctions targetFunctions, SupportedTapEvents supportedTapEvents) {

    }

    @Override
    public void sourceFunctions(SourceFunctions sourceFunctions) {

    }
}
