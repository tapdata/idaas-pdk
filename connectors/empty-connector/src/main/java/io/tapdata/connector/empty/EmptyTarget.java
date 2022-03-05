package io.tapdata.connector.empty;

import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionTestResult;
import io.tapdata.pdk.apis.entity.SupportedTapEvents;
import io.tapdata.pdk.apis.entity.ddl.TapTable;
import io.tapdata.pdk.apis.entity.ddl.TapTableOptions;
import io.tapdata.pdk.apis.entity.dml.TapRecordEvent;
import io.tapdata.pdk.apis.functions.consumers.TapListConsumer;
import io.tapdata.pdk.apis.functions.consumers.TapWriteListConsumer;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.apis.TapTarget;
import io.tapdata.pdk.apis.annotations.TapConnector;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.TargetFunctions;

import java.util.List;
import java.util.function.Consumer;

@TapConnector("target.json")
public class EmptyTarget implements TapTarget {

    @Override
    public void destroy() {

    }


    @Override
    public void discoverSchema(TapConnectionContext databaseContext, TapListConsumer<TapTableOptions> tapReadOffsetConsumer) {

    }

    @Override
    public ConnectionTestResult connectionTest(TapConnectionContext databaseContext) {
        return null;
    }

    @Override
    public void init(TapConnectorContext connectorContext, TapNodeSpecification specification) {

    }

    /**
     *
     * @param targetFunctions
     * @param supportedTapEvents
     */
    @Override
    public void targetFunctions(TargetFunctions targetFunctions, SupportedTapEvents supportedTapEvents) {
        targetFunctions.withDMLFunction(this::handleDML);
//        targetFunctions.withDDLFunction(this::handleDDL);
//        targetFunctions.withQueryByFilterFunction(this::queryByFilter);
//        targetFunctions.withTransactionFunction(this::handleTransaction);
    }

    /**
     *
     * @param connectorContext
     * @param tapRecordEvents
     * @param consumer
     */
    private void handleDML(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapWriteListConsumer<TapRecordEvent> consumer) {
    }
}
