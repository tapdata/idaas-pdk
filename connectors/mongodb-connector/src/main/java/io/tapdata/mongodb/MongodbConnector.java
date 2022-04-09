package io.tapdata.mongodb;

import com.mongodb.client.*;
import com.mongodb.client.internal.MongoBatchCursorAdapter;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.codec.TapCodecRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.*;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapArray;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.mongodb.bean.MongoDBConfig;
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
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Updates.set;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Different Connector need use different "spec.json" file with different pdk id which specified in Annotation "TapConnectorClass"
 * In parent class "ConnectorBase", provides many simplified methods to develop connector
 */
@TapConnectorClass("spec.json")
public class MongodbConnector extends ConnectorBase implements TapConnector {
    public static final String TAG = MongodbConnector.class.getSimpleName();
    private final AtomicLong counter = new AtomicLong();
    private final AtomicBoolean isShutDown = new AtomicBoolean(false);
    private MongoDBConfig mongoConfig;
    private MongoClient mongoClient;
    private MongoDatabase mongoDatabase;
    MongoCollection<Document> mongoCollection;
    private DataMap dataMap = new DataMap();
    private ObjectId batchOffsetId = null;
    private Long documentCount = null;

    private void initConnection(DataMap config) {
        try {
            mongoConfig = MongoDBConfig.load(config);
            if (mongoClient == null) {
                mongoClient = MongoClients.create("mongodb://" + mongoConfig.getHost() + ":" + mongoConfig.getPort());
                mongoDatabase = mongoClient.getDatabase(mongoConfig.getDatabase());
                mongoCollection = mongoDatabase.getCollection(mongoConfig.getCollection());
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Create Connection Failed!");
        }
    }


    /**
     * The method invocation life circle is below,
     * initiated -> discoverSchema -> destroy -> ended
     * <p>
     * You need to create the connection to your data source and release the connection in destroy method.
     * In connectionContext, you can get the connection config which is the user input for your connection application, described in your json file.
     * <p>
     * Consumer can accept multiple times, especially huge number of table list.
     * This is sync method, once the method return, Incremental engine will consider schema has been discovered.
     *
     * @param connectionContext
     * @param consumer
     */
    @Override
    public void discoverSchema(TapConnectionContext connectionContext, Consumer<List<TapTable>> consumer) {
        initConnection(connectionContext.getConnectionConfig());
        MongoIterable<String> collectionNames = mongoDatabase.listCollectionNames();
        for (String collectionName : collectionNames) {
            consumer.accept(list(table(collectionName)));
        }

    }

    /**
     * The method invocation life circle is below,
     * initiated -> connectionTest -> destroy -> ended
     * <p>
     * You need to create the connection to your data source and release the connection in destroy method.
     * In connectionContext, you can get the connection config which is the user input for your connection application, described in your json file.
     * <p>
     * consumer can call accept method multiple times to test different items
     *
     * @param connectionContext
     * @return
     */
    @Override
    public void connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        initConnection(connectionContext.getConnectionConfig());
        consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY));
        //Login test
        //TODO execute login test here
        consumer.accept(testItem(TestItem.ITEM_LOGIN, TestItem.RESULT_SUCCESSFULLY));
        //Read test
        //TODO execute read test by checking role permission
        consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY));
        //Write test
        //TODO execute write test by checking role permission
        consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY));

        //When test failed
//        consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, "Connection refused"));
        //When test successfully, but some warn is reported.
//        consumer.accept(testItem(TestItem.ITEM_READ_LOG, TestItem.RESULT_SUCCESSFULLY_WITH_WARN, "CDC not enabled, please check your database settings"));
    }

    /**
     * Register connector capabilities here.
     * <p>
     * To be as a target, please implement WriteRecordFunction and QueryByFilterFunction.
     * If the database need create table before record insertion, then please implement CreateTableFunction and DropTableFunction,
     * Incremental engine will generate the data types for each field base on incoming records for CreateTableFunction to create the table.
     * <p>
     * If defined data types in spec.json is not covered all the TapValue,
     * like TapTimeValue, TapMapValue, TapDateValue, TapArrayValue, TapYearValue, TapNumberValue, TapBooleanValue, TapDateTimeValue, TapBinaryValue, TapRawValue, TapStringValue,
     * then please provide the custom codec for missing TapValue by using codeRegistry.
     * This is only needed when database need create table before insert records.
     *
     * @param connectorFunctions
     * @param codecRegistry
     */
    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecRegistry codecRegistry) {
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportQueryByFilter(this::queryByFilter);


        codecRegistry.registerFromTapValue(TapRawValue.class, "json", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null)
                return toJson(tapRawValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapArrayValue.class, "json", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null)
                return toJson(tapRawValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapMapValue.class, "json", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null)
                return toJson(tapRawValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapTimeValue.class, "json", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null)
                return toJson(tapRawValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, "json", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null)
                return toJson(tapRawValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapDateValue.class, "json", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null)
                return toJson(tapRawValue.getValue());
            return "null";
        });

        connectorFunctions.supportBatchRead(this::batchRead);
        connectorFunctions.supportBatchCount(this::batchCount);
        connectorFunctions.supportBatchOffset(this::batchOffset);


    }

    /**
     * The method invocation life circle is below,
     * initiated ->
     * if(needCreateTable)
     * createTable
     * if(needClearTable)
     * clearTable
     * if(needDropTable)
     * dropTable
     * writeRecord
     * -> destroy -> ended
     *
     * @param connectorContext
     * @param tapRecordEvents
     * @param writeListResultConsumer
     */
    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) {
        initConnection(connectorContext.getConnectionConfig());
        //Below is sample code to print received events which suppose to write to database.
        AtomicLong inserted = new AtomicLong(0); //insert count
        AtomicLong updated = new AtomicLong(0); //update count
        AtomicLong deleted = new AtomicLong(0); //delete count

        TapTable tapTable = connectorContext.getTable();
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        List<Document> insertList = new ArrayList<>();
        UpdateOptions options = new UpdateOptions().upsert(true);

        for (TapRecordEvent recordEvent : tapRecordEvents) {
            if (recordEvent instanceof TapInsertRecordEvent) {
                TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) recordEvent;
                insertList.add(new Document(insertRecordEvent.getAfter()));
                inserted.incrementAndGet();
                PDKLogger.info(TAG, "Record Write TapInsertRecordEvent {}", toJson(recordEvent));
            } else if (recordEvent instanceof TapUpdateRecordEvent) {
                if(!insertList.isEmpty()){
                    mongoCollection.insertMany(insertList);
                }
                TapUpdateRecordEvent updateRecordEvent = (TapUpdateRecordEvent) recordEvent;
                Map<String, Object> after = updateRecordEvent.getAfter();

                List<Bson> filterAfter = new ArrayList<>();
                List<Bson> updateAfter = new ArrayList<>();


                for (Map.Entry<String, Object> entry : after.entrySet()) {
                    String fieldName = entry.getKey();
                    if (nameFieldMap.get(fieldName).getPrimaryKey() != null && nameFieldMap.get(fieldName).getPrimaryKey()) {
                        filterAfter.add(eq(entry.getKey(), entry.getValue()));
                    } else {
                        updateAfter.add(set(entry.getKey(), entry.getValue()));
                    }
                }

                mongoCollection.updateOne(and(filterAfter.toArray(new Bson[0])),Updates.combine(updateAfter.toArray(new Bson[0])),options);
                updated.incrementAndGet();
                PDKLogger.info(TAG, "Record Write TapUpdateRecordEvent {}", toJson(recordEvent));
            } else if (recordEvent instanceof TapDeleteRecordEvent) {
                if(!insertList.isEmpty()){
                    mongoCollection.insertMany(insertList);
                }
                TapDeleteRecordEvent deleteRecordEvent = (TapDeleteRecordEvent) recordEvent;
                Map<String, Object> before = deleteRecordEvent.getBefore();


                List<Bson> filterBefore = new ArrayList<>();
                for (Map.Entry<String, Object> entry : before.entrySet()) {
                    filterBefore.add(eq(entry.getKey(), entry.getValue()));
                }

                mongoCollection.deleteOne(and(filterBefore.toArray(new Bson[0])));
                deleted.incrementAndGet();
                PDKLogger.info(TAG, "Record Write TapDeleteRecordEvent {}", toJson(recordEvent));
            }
        }
        if(!insertList.isEmpty()){
            mongoCollection.insertMany(insertList);
        }
        //Need to tell incremental engine the write result
        writeListResultConsumer.accept(writeListResult()
                .insertedCount(inserted.get())
                .modifiedCount(updated.get())
                .removedCount(deleted.get()));
    }

    /**
     * The method will mainly be used by TDD tests. To verify the record has writen correctly or not.
     *
     * @param connectorContext
     * @param filters          Multple fitlers, need return multiple filter results
     * @param listConsumer     tell incremental engine the filter results according to filters
     */
    private void queryByFilter(TapConnectorContext connectorContext, List<TapFilter> filters, Consumer<List<FilterResult>> listConsumer) {
        //Filter is exactly match.
        //If query by the filter, no value is in database, please still create a FitlerResult with null value in it. So that incremental engine can understand the filter has no value.
        Set<String> columnNames = connectorContext.getTable().getNameFieldMap().keySet();
        if(filters != null) {
            List<FilterResult> filterResults = new ArrayList<>();
            for(TapFilter filter : filters) {
                List<Bson> bsonList = new ArrayList<>();
                for (Map.Entry<String, Object> entry : filter.getMatch().entrySet()) {
                    bsonList.add(eq(entry.getKey(), entry.getValue()));
                }
                MongoCursor<Document> cursor = mongoCollection.find(and(bsonList.toArray(new Bson[0]))).iterator();
                FilterResult filterResult = new FilterResult();
                while(cursor.hasNext()){
                    Document document = cursor.next();
                    DataMap resultMap = new DataMap();
                    for (String columnName : columnNames) {
                        resultMap.put(columnName, document.get(columnName));
                    }
                    filterResult.setResult(resultMap);
                }
                filterResult.filter(filter);
                filterResults.add(filterResult);
            }
            listConsumer.accept(filterResults);
        }
    }

    /**
     * The method invocation life circle is below,
     * initiated ->
     * if(batchEnabled)
     * batchCount -> batchRead
     * if(streamEnabled)
     * streamRead
     * -> destroy -> ended
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
        initConnection(connectorContext.getConnectionConfig());
        if (documentCount == null) documentCount = mongoCollection.countDocuments();
        if (documentCount == dataMap.get("batchReadOffset")) return 0L;
//        if ((ObjectId)offset != batchOffsetId)
        return 1000L;
    }

    /**
     * The method invocation life circle is below,
     * initiated ->
     * if(batchEnabled)
     * batchCount -> batchRead
     * if(streamEnabled)
     * streamRead
     * -> destroy -> ended
     * <p>
     * In connectorContext,
     * you can get the connection/node config which is the user input for your connection/node application, described in your json file.
     * current instance is serving for the table from connectorContext.
     *
     * @param connectorContext
     * @param offset
     * @param tapReadOffsetConsumer
     */
    private void batchRead(TapConnectorContext connectorContext, Object offset, int batchSize, Consumer<List<TapEvent>> tapReadOffsetConsumer) {
        batchOffsetId = (ObjectId) offset;

        //TODO batch read all records from database, use consumer#accept to send to incremental engine.
        initConnection(connectorContext.getConnectionConfig());
        MongoBatchCursorAdapter<Document> mongoCursor;
        if (offset == null) {
            mongoCursor = (MongoBatchCursorAdapter<Document>) mongoCollection.find().batchSize(batchSize).iterator();
        } else {
            mongoCursor = (MongoBatchCursorAdapter<Document>) mongoCollection.find(gt("_id",batchOffsetId)).batchSize(batchSize).iterator();
        }

        //Below is sample code to generate records directly.
        Document document = null;
        while (mongoCursor.hasNext()) {
            List<TapEvent> tapEvents = list();
            for (int i = 0; i < batchSize; i++) {
                if (!mongoCursor.hasNext()){
                    batchSize = i + 1;
                    break;
                }
                Map<String, Object> after = new DataMap();
                document = mongoCursor.next();
                after.putAll(document);
                TapInsertRecordEvent recordEvent = insertRecordEvent(after, connectorContext.getTable());
                tapEvents.add(recordEvent);
//                if (mongoCursor.available() == 0) break;
            }
            counter.set(counter.get() + batchSize);
            if (document != null) {
                batchOffsetId = (ObjectId) document.get("_id");
                dataMap.put("batchReadOffsetObjectId", batchOffsetId);
                dataMap.put("batchReadOffset", counter.get());
            }
            tapReadOffsetConsumer.accept(tapEvents);
        }
        PDKLogger.info(TAG, "batchRead Count {}", counter.get());
    }

    /**
     * The method invocation life circle is below,
     * initiated ->
     * if(batchEnabled)
     * batchCount -> batchRead
     * if(streamEnabled)
     * streamRead
     * -> destroy -> ended
     * <p>
     * In connectorContext,
     * you can get the connection/node config which is the user input for your connection/node application, described in your json file.
     * current instance is serving for the table from connectorContext.
     *
     * @param connectorContext //     * @param offset
     *                         //     * @param consumer
     */
//    private void streamRead(TapConnectorContext connectorContext, Object offset, Consumer<List<TapEvent>> consumer) {
//        //TODO using CDC APi or log to read stream records from database, use consumer#accept to send to incremental engine.
//
//        //Below is sample code to generate stream records directly
//        while (!isShutDown.get()) {
//            List<TapEvent> tapEvents = list();
//            for (int i = 0; i < 10; i++) {
//                TapInsertRecordEvent event = insertRecordEvent(map(
//                        entry("id", counter.incrementAndGet()),
//                        entry("description", "123"),
//                        entry("name", "123"),
//                        entry("age", 12)
//                ), connectorContext.getTable());
//                tapEvents.add(event);
//            }
//
//            sleep(1000L);
//            consumer.accept(tapEvents);
//        }
//    }

    private Object batchOffset(TapConnectorContext connectorContext) throws Throwable {
        return batchOffsetId;
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
        if (mongoClient != null) {
            mongoClient.close();
        }
        isShutDown.set(true);
    }
}
