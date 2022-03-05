package io.tapdata.connector.empty;

import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionTestResult;
import io.tapdata.pdk.apis.entity.ddl.TapField;
import io.tapdata.pdk.apis.entity.ddl.TapTable;
import io.tapdata.pdk.apis.functions.consumers.TapListConsumer;
import io.tapdata.pdk.apis.functions.consumers.TapReadOffsetConsumer;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.apis.TapSource;
import io.tapdata.pdk.apis.annotations.TapConnector;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.SourceFunctions;
import io.tapdata.pdk.apis.entity.TapEvent;
import io.tapdata.pdk.apis.entity.dml.TapRecordEvent;
import io.tapdata.pdk.apis.entity.ddl.TapTableOptions;
import io.tapdata.pdk.apis.typemapping.TapType;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

@TapConnector("source.json")
public class EmptySource implements TapSource {
    private AtomicLong counter = new AtomicLong();
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
        TapTableOptions tableOptions1 = new TapTableOptions();
        TapTable table1 = new TapTable();
        LinkedHashMap<String, TapField> fieldMap = new LinkedHashMap<>();
        tableOptions1.setTable(table1);
        table1.setId("empty-table1");
        table1.setName("empty-table1");
        TapField idField = new TapField();
        idField.setName("id");
        idField.setTapType(TapType.Number.name());
        fieldMap.put(idField.getName(), idField);
        TapField aField = new TapField();
        aField.setName("a");
        aField.setTapType(TapType.String.name());
        fieldMap.put(aField.getName(), aField);
        TapField bField = new TapField();
        bField.setName("b");
        bField.setTapType(TapType.String.name());
        fieldMap.put(bField.getName(), bField);
        TapField cField = new TapField();
        cField.setName("c");
        cField.setTapType(TapType.String.name());
        table1.setNameFieldMap(fieldMap);
        tapListConsumer.accept(Arrays.asList(tableOptions1), null);
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
     * initiated -> init -> sourceFunctions/targetFunctions -> close -> ended
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
        return 20L;
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
    private void streamRead(TapConnectorContext connectorContext, Object offset, TapReadOffsetConsumer<TapEvent> tapReadOffsetConsumer) {
        for(int j = 0; j < 1; j++) {
            List<TapEvent> tapEvents = new ArrayList<>();
            for(int i = 0; i < 10; i++) {
              TapRecordEvent recordEvent = new TapRecordEvent();
              recordEvent.setType(TapRecordEvent.TYPE_INSERT);
              recordEvent.setTime(System.currentTimeMillis());
              int finalI = i;
              recordEvent.setAfter(new HashMap<String, Object>(){{
                put("id", counter.incrementAndGet());
                put("a", "123");
                put("b", "123");
                put("c", "123");
              }});
              tapEvents.add(recordEvent);
            }
            try {
                Thread.sleep(1000L);
                System.out.println(Thread.currentThread().getName() + " sleep 1000");
            } catch (InterruptedException interruptedException) {
                interruptedException.printStackTrace();
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
            for(int i = 0; i < 20; i++) {
                TapRecordEvent recordEvent = new TapRecordEvent();
                recordEvent.setType(TapRecordEvent.TYPE_INSERT);
                recordEvent.setTime(System.currentTimeMillis());
                int finalI = i;
                recordEvent.setAfter(new HashMap<String, Object>(){{
                  put("id", counter.incrementAndGet());
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
//        sourceFunctions.withBatchOffsetFunction(this::batchOffset);
//        sourceFunctions.withStreamOffsetFunction(this::streamOffset);
//        sourceFunctions.withWebHookReadFunction(this::webHook);
    }
}
