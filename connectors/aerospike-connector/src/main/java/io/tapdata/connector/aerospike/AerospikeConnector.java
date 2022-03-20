package io.tapdata.connector.aerospike;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.WritePolicy;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.tapdata.base.ConnectorBase;
import io.tapdata.connector.aerospike.bean.IRecord;
import io.tapdata.connector.aerospike.bean.TapAerospikeRecord;
import io.tapdata.connector.aerospike.utils.AerospikeSinkConfig;
import io.tapdata.connector.aerospike.utils.AerospikeStringSink;
import io.tapdata.entity.codec.TapCodecRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.*;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.common.DefaultMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.logger.PDKLogger;
import org.junit.Assert;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

@TapConnectorClass("spec.json")
public class AerospikeConnector extends ConnectorBase implements TapConnector {
    public static final String TAG = AerospikeConnector.class.getSimpleName();
    private final AtomicLong counter = new AtomicLong();
    private final AtomicBoolean isShutDown = new AtomicBoolean(false);

    private AerospikeSinkConfig sinkConfig;
    private AerospikeStringSink aerospikeStringSink;
    private final WritePolicy policy = new WritePolicy();

    public void initConnection(Map<String, Object> configMap) throws Exception {
        if (aerospikeStringSink != null && this.aerospikeStringSink.isConnected()) {
            aerospikeStringSink.client.close();
        }
        aerospikeStringSink = new AerospikeStringSink();
        sinkConfig = AerospikeSinkConfig.load(configMap);
        policy.timeoutDelay = 20;
        aerospikeStringSink.open(sinkConfig);

    }

    /**
     * The method invocation life circle is below,
     * initiated -> discoverSchema -> ended
     *
     * You need to create the connection to your data source and release the connection after usage in this method.
     * In connectionContext, you can get the connection config which is the user input for your connection application, described in your json file.
     *
     * Consumer can accept multiple times, especially huge number of table list.
     * This is sync method, once the method return, Flow engine will consider schema has been discovered.
     *
     * @param connectionContext
     * @param consumer
     */
    @Override
    public void discoverSchema(TapConnectionContext connectionContext, Consumer<List<TapTable>> consumer) {
        //TODO Load schema from database, connection information in connectionContext#getConnectionConfig
        //Sample code shows how to define tables with specified fields.
        consumer.accept(list(
                //Define first table
                table("aerospike-table1")
                        //Define a field named "id", origin field type, whether is primary key and primary key position
                        .add(field("id", "VARCHAR").isPrimaryKey(true))
        ));
    }

    @Override
    public void connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        //Assume below tests are successfully, below tests are recommended, but not required.
        //Connection test
        try {
            initConnection(connectionContext.getConnectionConfig());
        } catch (Exception e) {
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, "Connection refused"));
        }
        Assert.assertTrue(aerospikeStringSink.client.isConnected());
        consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY));

        //Login test
        //TODO execute login test here
        consumer.accept(testItem(TestItem.ITEM_LOGIN, TestItem.RESULT_SUCCESSFULLY));


        //Write test
        String mock_source_data = "{\"id\":1.0,\"description\":\"description123\",\"name\":\"name123\",\"age\":12.0}";

        DefaultMap defaultMap = this.fromJson(mock_source_data);
        // Aerospike does not store primary key by default
        String keyStr = defaultMap.get("id").toString();
        Key key = new Key(sinkConfig.getKeyspace(), sinkConfig.getKeySet(), keyStr);
        aerospikeStringSink.client.delete(policy, key);
        IRecord<String> tapAerospikeRecord = new TapAerospikeRecord(mock_source_data, keyStr);
        aerospikeStringSink.write(tapAerospikeRecord);

        consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY));

        //Read test
        Assert.assertEquals("{PK=1.0}", aerospikeStringSink.client.get(policy, key, "PK").bins.toString());
        Assert.assertEquals("{id=1.0}", aerospikeStringSink.client.get(policy, key, "id").bins.toString());
        Assert.assertEquals("{description=description123}", aerospikeStringSink.client.get(policy, key, "description").bins.toString());
        Assert.assertEquals("{name=name123}", aerospikeStringSink.client.get(policy, key, "name").bins.toString());
        Assert.assertEquals("{age=12.0}", aerospikeStringSink.client.get(policy, key, "age").bins.toString());

        aerospikeStringSink.client.delete(policy, key);

        consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY));

        //Read log test to check CDC capability
        //TODO execute read log test here  Aerospike log?
        consumer.accept(testItem(TestItem.ITEM_READ_LOG, TestItem.RESULT_SUCCESSFULLY));

        //When test failed
//        consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, "Connection refused"));
        //When test successfully, but some warn is reported.
        //        consumer.accept(testItem(TestItem.ITEM_READ_LOG, TestItem.RESULT_SUCCESSFULLY_WITH_WARN, "CDC not enabled, please check your database settings"));
    }

    /**
     * Register connector capabilities here.
     *
     * To be as a source, please implement at least one of batchReadFunction or streamReadFunction.
     * To be as a target, please implement WriteRecordFunction.
     * To be as a source and target, please implement the functions that source and target required.
     *
     * @param connectorFunctions
     * @param codecRegistry
     */
    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecRegistry codecRegistry) {
        connectorFunctions.supportBatchRead(this::batchRead);
        connectorFunctions.supportStreamRead(this::streamRead);
        connectorFunctions.supportBatchCount(this::batchCount);
        connectorFunctions.supportBatchOffset(this::batchOffset);
        connectorFunctions.supportStreamOffset(this::streamOffset);

        connectorFunctions.supportWriteRecord(this::writeRecord);

        //Below capabilities, developer can decide to implement or not.
//        connectorFunctions.supportCreateTable(this::createTable);
//        connectorFunctions.supportQueryByFilter(this::queryByFilter);
//        connectorFunctions.supportAlterTable(this::alterTable);
//        connectorFunctions.supportDropTable(this::dropTable);
//        connectorFunctions.supportClearTable(this::clearTable);
    }

    /**
     * This method will be invoked any time when Flow engine need to save stream offset.
     * If stream read has started, this method need return current stream offset, otherwise return null.
     *
     * @param offsetStartTime  specify the expected start time to return the offset. If null, return current offset.
     * @param connectorContext the node context in a DAG
     */
    Object streamOffset(TapConnectorContext connectorContext, Long offsetStartTime) throws Throwable {
        //If don't support return stream offset by offsetStartTime, please throw NotSupportedException to let Flow engine knows, otherwise the result will be unpredictable.
//        if(offsetStartTime != null)
//            throw new NotSupportedException();
        //TODO return stream offset
        return null;
    }

    /**
     * The method will be invoked any time when Flow engine need to save batch offset.
     * If batch read has started, this method need return current batch offset, otherwise return null.
     *
     * @param connectorContext the node context in a DAG
     * @return
     */
    private Object batchOffset(TapConnectorContext connectorContext) {
        return null;
    }

    /**
     * The method invocation life circle is below,
     * initiated ->
     *  if(needCreateTable)
     *      createTable
     *  if(needClearTable)
     *      clearTable
     *  writeRecord
     * -> destroy -> ended
     *
     * @param connectorContext
     * @param tapRecordEvents
     * @param writeListResultConsumer
     */
    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Exception {
        //write records into database
        if (aerospikeStringSink == null) {
            initConnection(connectorContext.getConnectionConfig());
        }

        //Below is sample code to print received events which suppose to write to database.
        AtomicLong inserted = new AtomicLong(0); //insert count
        AtomicLong updated = new AtomicLong(0); //update count
        AtomicLong deleted = new AtomicLong(0); //delete count
        for (TapRecordEvent recordEvent : tapRecordEvents) {
            String json = toJson(recordEvent);
            DefaultMap defaultMap = JSON.parseObject(json, DefaultMap.class);
            JSONObject after_json_obj = (JSONObject) defaultMap.get("after");

            TapTable table = recordEvent.getTable();
            LinkedHashMap<String, TapField> nameFieldMap = table.getNameFieldMap();
            String keyStr = null;
            for (TapField field : nameFieldMap.values()) {
                if (field.getPrimaryKey())
                    keyStr = after_json_obj.get(field.getName()).toString();
                    break;
            }

            String after_json = after_json_obj.toString();
            IRecord<String> tapAerospikeRecord = new TapAerospikeRecord(after_json, keyStr);
            if (recordEvent instanceof TapInsertRecordEvent) {
                inserted.incrementAndGet();
                aerospikeStringSink.write(tapAerospikeRecord);
                PDKLogger.info(TAG, "Record Write TapInsertRecordEvent {}", toJson(recordEvent));
            } else if (recordEvent instanceof TapUpdateRecordEvent) {
                updated.incrementAndGet(); // TODO success update ?
                Key key = new Key(sinkConfig.getKeyspace(), sinkConfig.getKeySet(), keyStr);
                Record record = aerospikeStringSink.client.get(policy, key);
                if (record == null) continue;
                aerospikeStringSink.write(tapAerospikeRecord);
                PDKLogger.info(TAG, "Record Write TapUpdateRecordEvent {}", toJson(recordEvent));
            } else if (recordEvent instanceof TapDeleteRecordEvent) {
                deleted.incrementAndGet(); // TODO delete update ?
                Key key = new Key(sinkConfig.getKeyspace(), sinkConfig.getKeySet(), keyStr);
                Record record = aerospikeStringSink.client.get(policy, key);
                if (record == null) continue;
                aerospikeStringSink.client.delete(policy, key);
                PDKLogger.info(TAG, "Record Write TapDeleteRecordEvent {}", toJson(recordEvent));
            }
        }

        //Need to tell flow engine the write result
        writeListResultConsumer.accept(writeListResult()
                .insertedCount(inserted.get())
                .modifiedCount(updated.get())
                .removedCount(deleted.get()));
    }

    /**
     * The method invocation life circle is below,
     * initiated ->
     *  if(batchEnabled)
     *      batchCount -> batchRead
     *  if(streamEnabled)
     *      streamRead
     * -> destroy -> ended
     *
     * In connectorContext,
     * you can get the connection/node config which is the user input for your connection/node application, described in your json file.
     * current instance is serving for the table from connectorContext.
     *
     * @param connectorContext
     * @param offset
     * @return
     */
    private long batchCount(TapConnectorContext connectorContext, Object offset) {
        //TODO Count the batch size.
        return 20L;
    }

    /**
     * The method invocation life circle is below,
     * initiated ->
     *  if(batchEnabled)
     *      batchCount -> batchRead
     *  if(streamEnabled)
     *      streamRead
     * -> destroy -> ended
     *
     * In connectorContext,
     * you can get the connection/node config which is the user input for your connection/node application, described in your json file.
     * current instance is serving for the table from connectorContext.
     *
     * @param connectorContext
     * @param offset
     * @param tapReadOffsetConsumer
     */
    private void batchRead(TapConnectorContext connectorContext, Object offset, int batchSize, Consumer<List<TapEvent>> tapReadOffsetConsumer) {
        //TODO batch read all records from database, use consumer#accept to send to flow engine.

        //Below is sample code to generate records directly.
        for (int j = 0; j < 1; j++) {
            List<TapEvent> tapEvents = list();
            for (int i = 0; i < 20; i++) {
                TapInsertRecordEvent recordEvent = insertRecordEvent(map(
                        entry("id", counter.incrementAndGet()),
                        entry("description", "123"),
                        entry("name", "123"),
                        entry("age", 12)
                ), connectorContext.getTable());
                tapEvents.add(recordEvent);
            }
            tapReadOffsetConsumer.accept(tapEvents);
        }
        counter.set(counter.get() + 1000);
    }

    /**
     * The method invocation life circle is below,
     * initiated ->
     *  if(batchEnabled)
     *      batchCount -> batchRead
     *  if(streamEnabled)
     *      streamRead
     * -> destroy -> ended
     *
     * In connectorContext,
     * you can get the connection/node config which is the user input for your connection/node application, described in your json file.
     * current instance is serving for the table from connectorContext.
     *
     * @param connectorContext
     * @param offset
     * @param consumer
     */
    private void streamRead(TapConnectorContext connectorContext, Object offset, Consumer<List<TapEvent>> consumer) {
        //TODO using CDC APi or log to read stream records from database, use consumer#accept to send to flow engine.

        //Below is sample code to generate stream records directly
        while (!isShutDown.get()) {
            List<TapEvent> tapEvents = list();
            for (int i = 0; i < 10; i++) {
                TapInsertRecordEvent event = insertRecordEvent(map(
                        entry("id", counter.incrementAndGet()),
                        entry("description", "123"),
                        entry("name", "123"),
                        entry("age", 12)
                ), connectorContext.getTable());
                tapEvents.add(event);
            }

            sleep(1000L);
            consumer.accept(tapEvents);
        }
    }

    /**
     * The method invocation life circle is below,
     * initiated -> sourceFunctions/targetFunctions -> destroy -> ended
     *
     * In connectorContext,
     * you can get the connection/node config which is the user input for your connection/node application, described in your json file.
     * current instance is serving for the table from connectorContext.
     */
    @Override
    public void destroy() {
        //TODO release resources
        aerospikeStringSink.client.close();
        isShutDown.set(true);
    }
}
