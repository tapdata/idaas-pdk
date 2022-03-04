package io.tapdata.connector.file;

import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionTestResult;
import io.tapdata.pdk.apis.entity.ddl.TapTableOptions;
import io.tapdata.pdk.apis.functions.consumers.TapListConsumer;
import io.tapdata.pdk.apis.functions.consumers.TapReadOffsetConsumer;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.apis.TapSource;
import io.tapdata.pdk.apis.annotations.TapConnector;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.SourceFunctions;
import io.tapdata.pdk.apis.entity.TapEvent;
import io.tapdata.pdk.apis.entity.dml.TapRecordEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


@TapConnector("source.json")
public class FileSource implements TapSource {
    /**
     * The method invocation life circle is below,
     * initiated -> allTables -> ended
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
     * initiated -> connectionTest -> init -> sourceFunctions/targetFunctions -> close -> ended
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
     * initiated -> connectionTest -> init ->
     *  if(batchEnabled)
     *      batchCount -> batchRead
     *  if(streamEnabled)
     *      streamRead
     * -> close -> ended
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
     * initiated -> connectionTest -> init ->
     *  if(batchEnabled)
     *      batchCount -> batchRead
     *  if(streamEnabled)
     *      streamRead
     * -> close -> ended
     *
     * In connectorContext,
     *  you can get the connection/node config which is the user input for your connection/node application, described in your json file.
     *  current instance is serving for the table from connectorContext.
     *
     * @param connectorContext
     * @param o
     * @param tapReadOffsetConsumer
     */
    private void streamRead(TapConnectorContext connectorContext, Object o, TapReadOffsetConsumer<TapEvent> tapReadOffsetConsumer) {
        for(int j = 0; j < 3; j++) {
            List<TapEvent> tapEvents = new ArrayList<>();
            for(int i = 0; i < 1000; i++) {
              TapRecordEvent recordEvent = new TapRecordEvent();
              recordEvent.setType(TapRecordEvent.TYPE_INSERT);
              recordEvent.setTime(System.currentTimeMillis());
              int finalI = i;
              recordEvent.setAfter(new HashMap<String, Object>(){{
                put("id", finalI * 1000);
                put("a", "123");
                put("b", "123");
                put("c", "123");
              }});
              tapEvents.add(recordEvent);
            }
            tapReadOffsetConsumer.accept(tapEvents, null, null, false);
        }
    }

    /**
     * The method invocation life circle is below,
     * initiated -> connectionTest -> init ->
     *  if(batchEnabled)
     *      batchCount -> batchRead
     *  if(streamEnabled)
     *      streamRead
     * -> close -> ended
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
        for(int j = 0; j < 1; j++) {
            List<TapEvent> tapEvents = new ArrayList<>();
            for(int i = 1000; i < 2000; i++) {
                TapRecordEvent recordEvent = new TapRecordEvent();
                recordEvent.setType(TapRecordEvent.TYPE_INSERT);
                recordEvent.setTime(System.currentTimeMillis());
                int finalI = i;
                recordEvent.setAfter(new HashMap<String, Object>(){{
                  put("id", finalI * 1000);
                  put("a", "123");
                  put("b", "123");
                  put("c", "123");
                }});
                tapEvents.add(recordEvent);
            }
            tapReadOffsetConsumer.accept(tapEvents, null, null, false);
        }
        tapReadOffsetConsumer.accept(null, null, null, true);
    }

    /**
     * The method invocation life circle is below,
     *  initiated -> connectionTest -> init -> sourceFunctions/targetFunctions -> close -> ended
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
     * The method invocation life circle is below,
     * initiated -> connectionTest -> init -> sourceFunctions/targetFunctions -> close -> ended
     *
     * sourceFunctions provide the way for you to register callback functions to implement features that you may want to implement.
     * like batch read, stream read, etc.
     *
     * @param sourceFunctions
     */
    @Override
    public void sourceFunctions(SourceFunctions sourceFunctions) {
        /**
         *
         */
        sourceFunctions.withBatchReadFunction(this::batchRead);
        sourceFunctions.withStreamReadFunction(this::streamRead);
        sourceFunctions.withBatchCountFunction(this::batchCount);
    }
}
