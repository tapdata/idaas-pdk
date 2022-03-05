package ${package};

import io.tapdata.base.ConnectorBase;
import io.tapdata.pdk.apis.TapSource;
import io.tapdata.pdk.apis.TapTarget;
import io.tapdata.pdk.apis.annotations.TapConnector;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionTestResult;
import io.tapdata.pdk.apis.entity.SupportedTapEvents;
import io.tapdata.pdk.apis.entity.TapEvent;
import io.tapdata.pdk.apis.entity.ddl.TapTableOptions;
import io.tapdata.pdk.apis.entity.dml.TapRecordEvent;
import io.tapdata.pdk.apis.functions.SourceFunctions;
import io.tapdata.pdk.apis.functions.TargetFunctions;
import io.tapdata.pdk.apis.functions.consumers.TapListConsumer;
import io.tapdata.pdk.apis.functions.consumers.TapReadOffsetConsumer;
import io.tapdata.pdk.apis.functions.consumers.TapWriteListConsumer;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;

import java.util.Arrays;
import java.util.List;

/**
 * Implemented both TapTarget and TapSource, means the connector can be Source and Target at the same time.
 * If the connector want to be only Source or Target, then only implement one interface, TapSource or TapTarget.
 *
 * Please revise "sourceAndTarget.json" under resources directory, to specify your connector's id, group, icon and user input form etc.
 */
@TapConnector("sourceAndTarget.json")
public class ${libName}SourceAndTarget extends ConnectorBase implements TapTarget, TapSource {

    /**
     * The method invocation life circle is below,
     * initiated -> connectionTest -> ended
     *
     * You need to create the connection to your data source and release the connection after usage in this method.
     * In connectionContext, you can get the connection config which is the user input for your connection application, described in your json file.
     *
     * @param connectionContext
     * @return
     */
    @Override
    public ConnectionTestResult connectionTest(TapConnectionContext connectionContext) {
        return null;
    }
    /**
     * The method invocation life circle is below,
     * initiated -> discoverSchema -> ended
     *
     * You need to create the connection to your data source and release the connection after usage in this method.
     * In connectionContext, you can get the connection config which is the user input for your connection application, described in your json file.
     *  @param connectionContext
     * @param tapListConsumer
     */
    @Override
    public void discoverSchema(TapConnectionContext connectionContext, TapListConsumer<TapTableOptions> tapListConsumer) {

    }
    /**
     * The method invocation life circle is below,
     * initiated -> init -> sourceFunctions/targetFunctions -> destroy -> ended
     *
     * In connectorContext,
     *  you can get the connection/node config which is the user input for your connection/node application, described in your json file.
     *  current instance is serving for the table from connectorContext.
     *
     * @param connectorContext
     * @param specification
     */
    @Override
    public void init(TapConnectorContext connectorContext, TapNodeSpecification specification) {

    }
    /**
     * The method invocation life circle is below,
     * initiated -> init ->
     *  if(batchEnabled)
     *      batchCount -> batchRead
     *  if(streamEnabled)
     *      streamRead
     * -> destroy -> ended
     *
     * In connectorContext,
     *  you can get the connection/node config which is the user input for your connection/node application, described in your json file.
     *  current instance is serving for the table from connectorContext.
     *
     * @param connectorContext
     * @param offset
     * @return
     */
    private long batchCount(TapConnectorContext connectorContext, Object offset) {
        return 0;
    }
    /**
     * The method invocation life circle is below,
     * initiated -> init ->
     *  if(batchEnabled)
     *      batchCount -> batchRead
     *  if(streamEnabled)
     *      streamRead
     * -> destroy -> ended
     *
     * In connectorContext,
     *  you can get the connection/node config which is the user input for your connection/node application, described in your json file.
     *  current instance is serving for the table from connectorContext.
     *
     * @param connectorContext
     * @param offset
     * @param tapReadOffsetConsumer
     */
    private void batchRead(TapConnectorContext connectorContext, Object offset, TapReadOffsetConsumer<TapEvent> tapReadOffsetConsumer) {

    }
    /**
     * The method invocation life circle is below,
     * initiated -> init ->
     *  if(batchEnabled)
     *      batchCount -> batchRead
     *  if(streamEnabled)
     *      streamRead
     * -> destroy -> ended
     *
     * In connectorContext,
     *  you can get the connection/node config which is the user input for your connection/node application, described in your json file.
     *  current instance is serving for the table from connectorContext.
     *
     * @param connectorContext
     * @param offset
     * @param tapReadOffsetConsumer
     */
    private void streamRead(TapConnectorContext connectorContext, Object offset, TapReadOffsetConsumer<TapEvent> tapReadOffsetConsumer) {

    }
    /**
     * The method invocation life circle is below,
     * initiated -> init ->
     *  handleDML
     * -> destroy -> ended
     *
     * In connectorContext,
     *  you can get the connection/node config which is the user input for your connection/node application, described in your json file.
     *  current instance is serving for the table from connectorContext.
     *
     * @param connectorContext
     * @param offset
     * @param tapReadOffsetConsumer
     */
    private void handleDML(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapWriteListConsumer<TapRecordEvent> tapReadOffsetConsumer) {

    }

    /**
     * The method invocation life circle is below,
     *  initiated -> init -> sourceFunctions/targetFunctions -> destroy -> ended
     *
     * In connectorContext,
     *  you can get the connection/node config which is the user input for your connection/node application, described in your json file.
     *  current instance is serving for the table from connectorContext.
     *
     */
    @Override
    public void destroy() {

    }

    /**
     * Implement the ability functions as a source connector.
     * If the ability function your don't provide in your connector, then don't register the function to Flow Engine.
     *
     * @param sourceFunctions
     */
    @Override
    public void sourceFunctions(SourceFunctions sourceFunctions) {
        sourceFunctions.withBatchCountFunction(this::batchCount);
        sourceFunctions.withBatchReadFunction(this::batchRead);
        sourceFunctions.withStreamReadFunction(this::streamRead);

        //Below ability function are not integrated yet. please don't implement.
//        sourceFunctions.withBatchOffsetFunction(this::batchOffset);
//        sourceFunctions.withStreamOffsetFunction(this::streamOffset);
//        sourceFunctions.withWebHookReadFunction(this::webHook);
    }
    /**
     * Implement the ability functions as a source connector.
     * If the ability function your don't provide in your connector, then don't register the function to Flow Engine.
     *
     * @param targetFunctions
     * @param supportedTapEvents specify the event types that the connector support. All events are supported by default.
     */
    @Override
    public void targetFunctions(TargetFunctions targetFunctions, SupportedTapEvents supportedTapEvents) {
        targetFunctions.withDMLFunction(this::handleDML);

        //Below ability function are not integrated yet. please don't implement.
//        targetFunctions.withDDLFunction(this::handleDDL);
//        targetFunctions.withQueryByFilterFunction(this::queryByFilter);
//        targetFunctions.withTransactionFunction(this::handleTransaction);
    }
}
