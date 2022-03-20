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
import io.tapdata.entity.utils.DefaultMap;
import io.tapdata.pdk.apis.TapConnector;
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
        if (aerospikeStringSink == null) {
//            if (aerospikeStringSink != null && this.aerospikeStringSink.isConnected()) {
//                aerospikeStringSink.client.close();
//            }
            aerospikeStringSink = new AerospikeStringSink();
            sinkConfig = AerospikeSinkConfig.load(configMap);
            policy.timeoutDelay = 20;
            aerospikeStringSink.open(sinkConfig);
        }
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
        DefaultMap connectionConfig = connectionContext.getConnectionConfig();
        String seedHosts = (String) connectionConfig.get("seedHosts");
        String keyspace = (String) connectionConfig.get("keyspace");
        String columnName = (String) connectionConfig.get("columnName");
        //TODO 在这里新建连接， 获得表信息。

        //获得真实的AS表数据
//        consumer.accept(list(
//                //Define first table
//                table("aerospike-table1")
//                        //Define a field named "id", origin field type, whether is primary key and primary key position
//                        .add(field("id", "VARCHAR").isPrimaryKey(true))
//        ));

        //TODO 释放连接
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

        //TODO 写入数据在真实环境下容易出大问题。 应该是通过检测有没有写权限来判定。
        String mock_source_data = "{\"id\":1.0,\"description\":\"description123\",\"name\":\"name123\",\"age\":12.0}";

        DefaultMap defaultMap = this.fromJson(mock_source_data);
        // Aerospike does not store primary key by default
        String keyStr = defaultMap.get("id").toString();
        Key key = new Key(sinkConfig.getKeyspace(), sinkConfig.getKeySet(), keyStr);
        aerospikeStringSink.client.delete(policy, key);
        IRecord<String> tapAerospikeRecord = new TapAerospikeRecord(mock_source_data, keyStr);
        aerospikeStringSink.write(tapAerospikeRecord);

        consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY));

        //TODO Assert只能用于测试代码， 正式代码不应该使用了。 这里如果出错应该使用consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_FAILED, "error message"));
        //Read test
        Assert.assertEquals("{PK=1.0}", aerospikeStringSink.client.get(policy, key, "PK").bins.toString());
        Assert.assertEquals("{id=1.0}", aerospikeStringSink.client.get(policy, key, "id").bins.toString());
        Assert.assertEquals("{description=description123}", aerospikeStringSink.client.get(policy, key, "description").bins.toString());
        Assert.assertEquals("{name=name123}", aerospikeStringSink.client.get(policy, key, "name").bins.toString());
        Assert.assertEquals("{age=12.0}", aerospikeStringSink.client.get(policy, key, "age").bins.toString());

        aerospikeStringSink.client.delete(policy, key);

        consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY));

        //Read log test to check CDC capability
        //TODO execute read log test here  Aerospike log? 作为目标端可以不用做这个测试， 这是源端做增量读取用的。 删掉就行
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
        //TODO 不做数据源， 就不用实现以下方法
//        connectorFunctions.supportBatchRead(this::batchRead);
//        connectorFunctions.supportStreamRead(this::streamRead);
//        connectorFunctions.supportBatchCount(this::batchCount);
//        connectorFunctions.supportBatchOffset(this::batchOffset);
//        connectorFunctions.supportStreamOffset(this::streamOffset);

        connectorFunctions.supportWriteRecord(this::writeRecord);

        //Below capabilities, developer can decide to implement or not.
//        connectorFunctions.supportCreateTable(this::createTable);
//        connectorFunctions.supportQueryByFilter(this::queryByFilter);
//        connectorFunctions.supportAlterTable(this::alterTable);
//        connectorFunctions.supportDropTable(this::dropTable);
//        connectorFunctions.supportClearTable(this::clearTable);
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
        //TODO 这样写会清晰点
        initConnection(connectorContext.getConnectionConfig());
//        if (aerospikeStringSink == null) {
//          initConnection(connectorContext.getConnectionConfig());
//        }

        //Below is sample code to print received events which suppose to write to database.
        AtomicLong inserted = new AtomicLong(0); //insert count
        AtomicLong updated = new AtomicLong(0); //update count
        AtomicLong deleted = new AtomicLong(0); //delete count
        for (TapRecordEvent recordEvent : tapRecordEvents) {
            //TODO 这个地方做的早了点， 应该在判断是insert， update或者delete时候取出after来toJson。 TapRecordEvent里面还有table， 是不必要tojson的
            String json = toJson(recordEvent);
            DefaultMap defaultMap = JSON.parseObject(json, DefaultMap.class);
            JSONObject after_json_obj = (JSONObject) defaultMap.get("after");

            TapTable table = recordEvent.getTable();
            LinkedHashMap<String, TapField> nameFieldMap = table.getNameFieldMap();
            String keyStr = null;
            for (TapField field : nameFieldMap.values()) {
                if (field.getPrimaryKey())
                    //TODO 两个问题， 多主键的时候应该拼接；field.primaryKeyPos有可能影响顺序， 不能采用nameFieldMap.values()顺序。
                    keyStr = after_json_obj.get(field.getName()).toString();
                    break;
            }

            String after_json = after_json_obj.toString();
            IRecord<String> tapAerospikeRecord = new TapAerospikeRecord(after_json, keyStr);


            IRecord<String> newRecord;
            if (recordEvent instanceof TapInsertRecordEvent) {

                TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) recordEvent;
                newRecord = new TapAerospikeRecord(toJson(insertRecordEvent.getAfter()), keyStr);
                aerospikeStringSink.write(newRecord);
                //插入成功之后再计数器加一
                inserted.incrementAndGet();

                PDKLogger.info(TAG, "Record Write TapInsertRecordEvent {}", toJson(recordEvent));
            } else if (recordEvent instanceof TapUpdateRecordEvent) {

                TapUpdateRecordEvent updateRecordEvent = (TapUpdateRecordEvent) recordEvent;
                newRecord = new TapAerospikeRecord(toJson(updateRecordEvent.getAfter()), keyStr);
                aerospikeStringSink.write(newRecord); //有更新能直接写入吧？ 不用再查出来， 直接覆盖写入就行

                updated.incrementAndGet(); // TODO success update ?

//                Key key = new Key(sinkConfig.getKeyspace(), sinkConfig.getKeySet(), keyStr);
//                Record record = aerospikeStringSink.client.get(policy, key);
//                if (record == null) continue;
//                aerospikeStringSink.write(tapAerospikeRecord);
                PDKLogger.info(TAG, "Record Write TapUpdateRecordEvent {}", toJson(recordEvent));
            } else if (recordEvent instanceof TapDeleteRecordEvent) {
                Key key = new Key(sinkConfig.getKeyspace(), sinkConfig.getKeySet(), keyStr);
//                Record record = aerospikeStringSink.client.get(policy, key);
//                if (record == null) continue;
                aerospikeStringSink.client.delete(policy, key);
                deleted.incrementAndGet(); // TODO delete update ?
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
