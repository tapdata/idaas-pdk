package io.tapdata.connector.aerospike;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.WritePolicy;
import io.tapdata.base.ConnectorBase;
import io.tapdata.connector.aerospike.bean.AerospikeNamespaces;
import io.tapdata.connector.aerospike.bean.AerospikeSet;
import io.tapdata.connector.aerospike.bean.IRecord;
import io.tapdata.connector.aerospike.bean.TapAerospikeRecord;
import io.tapdata.connector.aerospike.utils.AerospikeSinkConfig;
import io.tapdata.connector.aerospike.utils.AerospikeStringSink;
import io.tapdata.entity.codec.TapCodecRegistry;
import io.tapdata.entity.event.dml.*;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.value.DateTime;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.FilterResult;
import io.tapdata.pdk.apis.entity.TapFilter;
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
    private final AtomicBoolean isShutDown = new AtomicBoolean(false);

    private AerospikeSinkConfig sinkConfig;
    private AerospikeStringSink aerospikeStringSink;
    private final WritePolicy policy = new WritePolicy();

    public void initConnection(Map<String, Object> configMap) throws Exception {
        if (aerospikeStringSink == null) {
            aerospikeStringSink = new AerospikeStringSink();
            sinkConfig = AerospikeSinkConfig.load(configMap);
            policy.timeoutDelay = 20;
            aerospikeStringSink.open(sinkConfig);
        }
    }

    /**
     * The method invocation life circle is below,
     * initiated -> discoverSchema -> ended
     * <p>
     * You need to create the connection to your data source and release the connection after usage in this method.
     * In connectionContext, you can get the connection config which is the user input for your connection application, described in your json file.
     * <p>
     * Consumer can accept multiple times, especially huge number of table list.
     * This is sync method, once the method return, Flow engine will consider schema has been discovered.
     *
     * @param connectionContext
     * @param consumer
     */
    @Override
    public void discoverSchema(TapConnectionContext connectionContext, Consumer<List<TapTable>> consumer) {
        // Load schema from database, connection information in connectionContext#getConnectionConfig

        try {
            initConnection(connectionContext.getConnectionConfig());
        } catch (Exception e) {
            throw new RuntimeException("Create Connection Failed!");
        }

        // 获得表信息,获得真实的AS表数据
        String namespace = sinkConfig.getKeyspace();
        ArrayList<AerospikeSet> sets = AerospikeNamespaces.getSets(aerospikeStringSink.client, namespace);
        if (sets == null) {
            throw new RuntimeException(namespace + " is not exist!");
        }
        for (AerospikeSet set : sets) {
            consumer.accept(list(table(set.getSetName())));
        }

        try {
            aerospikeStringSink.close();
            aerospikeStringSink = null;
        } catch (Exception e) {
            throw new RuntimeException("Release Connection Failed!");
        }
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
        Assert.assertTrue(aerospikeStringSink.isConnected());
        consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY));

        //Login test
        //TODO execute login test here
        consumer.accept(testItem(TestItem.ITEM_LOGIN, TestItem.RESULT_SUCCESSFULLY));


        //Write test
        //TODO execute Write test here
        //TODO 检测写权限
        consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY));

        //Read test
        //TODO execute Read test here
        consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY));


        try {
            aerospikeStringSink.close();
        } catch (Exception e) {
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, "Connection Close failed"));
        }

        //When test successfully, but some warn is reported.
        //        consumer.accept(testItem(TestItem.ITEM_READ_LOG, TestItem.RESULT_SUCCESSFULLY_WITH_WARN, "CDC not enabled, please check your database settings"));
    }

    /**
     * Register connector capabilities here.
     * <p>
     * To be as a source, please implement at least one of batchReadFunction or streamReadFunction.
     * To be as a target, please implement WriteRecordFunction.
     * To be as a source and target, please implement the functions that source and target required.
     *
     * @param connectorFunctions
     * @param codecRegistry
     */
    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecRegistry codecRegistry) {
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportQueryByFilter(this::queryByFilter);
    }


    private String generateASPrimaryKey(Map<String, Object> recordMap, Collection<String> primaryKeyNames, Character splitSymbol) {
        StringBuilder builder = new StringBuilder();
        for (String fieldName : primaryKeyNames) {
            builder.append(fieldName);
            builder.append(splitSymbol);
            builder.append(recordMap.get(fieldName).toString());
            builder.append(splitSymbol);
        }
        if (builder.length() != 0) builder.delete(builder.length() - 1, builder.length());
        return builder.toString();
    }

    /**
     *
     * @param connectorContext
     * @param filters
     * @param listConsumer
     */
    private void queryByFilter(TapConnectorContext connectorContext, List<TapFilter> filters, Consumer<List<FilterResult>> listConsumer) {
        String keySet = connectorContext.getTable().getName();
        LinkedHashMap<String, TapField> nameFieldMap = connectorContext.getTable().getNameFieldMap();
        Collection<String> primaryKeys = connectorContext.getTable().primaryKeys();
        List<FilterResult> result = new LinkedList<>();
        for (TapFilter filter : filters) {
            FilterResult filterResult = new FilterResult();
            String key = generateASPrimaryKey(filter.getMatch(), primaryKeys, '_');
            try {
                Record filterRecordResult = aerospikeStringSink.read(keySet, key);
                if (filterRecordResult != null) {
                    filterResult.setResult(filterRecordResult.bins);
                }
                result.add(filterResult);
            } catch (AerospikeException aerospikeException) {
                filterResult.setError(aerospikeException);
            }
        }
        listConsumer.accept(result);
    }

    /**
     * The method invocation life circle is below,
     * initiated ->
     * if(needCreateTable)
     * createTable
     * if(needClearTable)
     * clearTable
     * writeRecord
     * -> destroy -> ended
     *
     * @param connectorContext
     * @param tapRecordEvents
     * @param writeListResultConsumer
     */
    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Exception {
        //write records into database
        initConnection(connectorContext.getConnectionConfig());

        //Below is sample code to print received events which suppose to write to database.
        AtomicLong inserted = new AtomicLong(0); //insert count
        AtomicLong updated = new AtomicLong(0); //update count
        AtomicLong deleted = new AtomicLong(0); //delete count
        for (TapRecordEvent recordEvent : tapRecordEvents) {
            TapTable targetTable = connectorContext.getTable();
            Collection<String> primaryKeys = targetTable.primaryKeys();

            String keySet = targetTable.getName();
            String newKey;
            IRecord<String> newRecord;
            LinkedHashMap<String, TapField> nameFieldMap = targetTable.getNameFieldMap();
            if (recordEvent instanceof TapInsertRecordEvent) {
                TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) recordEvent;
                Map<String, Object> after = insertRecordEvent.getAfter();
                for (String columnName : nameFieldMap.keySet()) {
                    TapField tapField = nameFieldMap.get(columnName);
                    Object value = after.get(columnName);
                    if (value instanceof DateTime) {
                        // TODO 依据不同的TapField进行不同类型的格式化
                        String dateValue = this.formatTapDateTime((DateTime) value, "yyyy-MM-dd HH:mm:ss");
                        after.put(columnName, dateValue);
                    }
                }
                newKey = generateASPrimaryKey(after, primaryKeys, '_');
                newRecord = new TapAerospikeRecord(toJson(after), newKey);
                aerospikeStringSink.write(newRecord, keySet);
                inserted.incrementAndGet();
            } else if (recordEvent instanceof TapUpdateRecordEvent) {
                TapUpdateRecordEvent updateRecordEvent = (TapUpdateRecordEvent) recordEvent;
                Map<String, Object> after = updateRecordEvent.getAfter();
                newKey = generateASPrimaryKey(after, primaryKeys, '_');
                newRecord = new TapAerospikeRecord(toJson(after), newKey);
                aerospikeStringSink.write(newRecord, keySet);
                updated.incrementAndGet();
            } else if (recordEvent instanceof TapDeleteRecordEvent) {
                TapDeleteRecordEvent deleteRecordEvent = (TapDeleteRecordEvent) recordEvent;
                Map<String, Object> before = deleteRecordEvent.getBefore();
                newKey = generateASPrimaryKey(before, primaryKeys, '_');
                Key key = new Key(sinkConfig.getKeyspace(), keySet, newKey);
                aerospikeStringSink.client.delete(policy, key);
                deleted.incrementAndGet();
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
     * <p>
     * In connectorContext,
     * you can get the connection/node config which is the user input for your connection/node application, described in your json file.
     * current instance is serving for the table from connectorContext.
     */
    @Override
    public void destroy() {
        if (aerospikeStringSink != null) {
            try {
                aerospikeStringSink.close();
                aerospikeStringSink = null;
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("release Connection Failed!");
            }
        }

        isShutDown.set(true);
    }
}
