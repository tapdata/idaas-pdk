package io.tapdata.connector.empty;

import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.codec.TapCodecRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertDMLEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

@TapConnectorClass("source.json")
public class EmptySource extends ConnectorBase implements TapConnector {
    private AtomicLong counter = new AtomicLong();

    /**
     * The method invocation life circle is below,
     * initiated -> allTables -> ended
     * <p>
     * You need to create the connection to your data source and release the connection after usage in this method.
     * In connectionContext, you can get the connection config which is the user input for your connection application, described in your json file.
     *
     * @param connectionContext
     * @param consumer
     */
    @Override
    public void discoverSchema(TapConnectionContext connectionContext, Consumer<List<TapTable>> consumer) {
        consumer.accept(list(
                table("empty-table1")
                        .add(field("id", tapString()))
                        .add(field("abc", tapString()))
                        .add(field("bbb", tapString()))
                        .add(field("number", tapNumber())),
                table("empty-table2")
                        .add(field("id", tapString()))
                        .add(field("abc", tapString()))
                        .add(field("bbb", tapString()))
                        .add(field("number", tapNumber()))
        ));
    }


    /**
     * The method invocation life circle is below,
     * initiated -> connectionTest -> ended
     * <p>
     * You need to create the connection to your data source and release the connection after usage in this method.
     * In connectionContext, you can get the connection config which is the user input for your connection application, described in your json file.
     *
     * @param connectionContext
     * @return
     */
    @Override
    public void connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        consumer.accept(testItem("OK", TestResult.Successfully));
    }


    /**
     * The method invocation life circle is below,
     * initiated -> init ->
     * if(batchEnabled)
     * batchCount -> batchRead
     * if(streamEnabled)
     * streamRead
     * -> close -> ended
     * <p>
     * In connectorContext,
     * you can get the connection/node config which is the user input for your connection/node application, described in your json file.
     * current instance is serving for the table from connectorContext.
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
     * if(batchEnabled)
     * batchCount -> batchRead
     * if(streamEnabled)
     * streamRead
     * -> close -> ended
     * <p>
     * In connectorContext,
     * you can get the connection/node config which is the user input for your connection/node application, described in your json file.
     * current instance is serving for the table from connectorContext.
     *
     * @param connectorContext
     * @param offset
     * @param consumer
     */
    private void streamRead(TapConnectorContext connectorContext, Object offset, Consumer<List<TapEvent>> consumer) {
        for (int j = 0; j < 1; j++) {
            List<TapEvent> tapEvents = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                TapInsertDMLEvent recordEvent = new TapInsertDMLEvent();
                recordEvent.setTime(System.currentTimeMillis());
                int finalI = i;
                recordEvent.setAfter(new HashMap<String, Object>() {{
                    put("id", counter.incrementAndGet());
                    put("a", "123");
                    put("b", "123");
                    put("c", "123");
                    put("abc", "dsafasdf");
                    put("bbb", "aaaaa");
                    put("number", 123123123);
                }});
                tapEvents.add(recordEvent);
            }
            try {
                Thread.sleep(1000L);
                System.out.println(Thread.currentThread().getName() + " sleep 1000");
            } catch (InterruptedException interruptedException) {
                interruptedException.printStackTrace();
            }
            consumer.accept(tapEvents);
        }
    }

    /**
     * The method invocation life circle is below,
     * initiated -> connectionTest -> init ->
     * if(batchEnabled)
     * batchCount -> batchRead
     * if(streamEnabled)
     * streamRead
     * -> close -> ended
     * <p>
     * In connectorContext,
     * you can get the connection/node config which is the user input for your connection/node application, described in your json file.
     * current instance is serving for the table from connectorContext.
     *
     * @param connectorContext
     * @param offset
     * @param tapReadOffsetConsumer
     */
    private void batchRead(TapConnectorContext connectorContext, Object offset, Consumer<List<TapEvent>> tapReadOffsetConsumer) {
        for (int j = 0; j < 1; j++) {
            List<TapEvent> tapEvents = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                TapInsertDMLEvent recordEvent = new TapInsertDMLEvent();
                recordEvent.setTime(System.currentTimeMillis());
                int finalI = i;
                recordEvent.setAfter(new HashMap<String, Object>() {{
                    put("id", counter.incrementAndGet());
                    put("a", "123");
                    put("b", "123");
                    put("c", "123");
                    put("abc", "123213213");
                    put("bbb", "44444444");
                    put("number", 555555);
                }});
                tapEvents.add(recordEvent);
            }
            tapReadOffsetConsumer.accept(tapEvents);
        }
    }

    /**
     * The method invocation life circle is below,
     * initiated -> connectionTest -> init -> sourceFunctions/targetFunctions -> close -> ended
     * <p>
     * In connectorContext,
     * you can get the connection/node config which is the user input for your connection/node application, described in your json file.
     * current instance is serving for the table from connectorContext.
     */
    @Override
    public void destroy() {

    }


    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecRegistry codecRegistry) {
        connectorFunctions.supportBatchRead(this::batchRead);
        connectorFunctions.supportStreamRead(this::streamRead);
        connectorFunctions.supportBatchCount(this::batchCount);

//        codecRegistry.registerToTapValue(Object.class)
    }
}
