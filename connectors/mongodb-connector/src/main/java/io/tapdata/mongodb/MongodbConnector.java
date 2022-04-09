package io.tapdata.mongodb;

import com.mongodb.MongoCredential;
import com.mongodb.client.*;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.OperationType;
import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.codec.TapCodecRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.*;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;

import static java.util.Arrays.asList;

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
import org.bson.BsonDocument;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Updates.set;
import static java.util.Collections.singletonList;

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
    private ObjectId batchOffsetId = null;
    private Long batchReadOffset;
    private Long documentCount = null;
    private final List<Bson> pipeline = singletonList(match(in("operationType", asList("insert", "update", "delete"))));
    private BsonDocument resumeToken = null;


    private void initConnection(DataMap config) {
        try {
            mongoConfig = MongoDBConfig.load(config);
            if (mongoClient == null) {
                mongoClient = MongoClients.create(mongoConfig.getUri());
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

        codecRegistry.registerToTapValue(ObjectId.class, value -> {
            ObjectId objValue = (ObjectId) value;
            return new TapStringValue(objValue.toHexString());
        });

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
        connectorFunctions.supportStreamRead(this::streamRead);
//        connectorFunctions.supportStreamOffset(this::streamOffset);

    }

//    Object streamOffset(TapConnectorContext connectorContext, Long offsetStartTime) throws Throwable {
//        //If don't support return stream offset by offsetStartTime, please throw NotSupportedException to let Flow engine knows, otherwise the result will be unpredictable.
////        if(offsetStartTime != null)
////            throw new NotSupportedException();
//        //TODO return stream offset
//        return null;
//    }


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
                if (!insertList.isEmpty()) {
                    mongoCollection.insertMany(insertList);
                }
                TapUpdateRecordEvent updateRecordEvent = (TapUpdateRecordEvent) recordEvent;
                Map<String, Object> after = updateRecordEvent.getAfter();

                List<Bson> filterAfter = new ArrayList<>();
                List<Bson> updateAfter = new ArrayList<>();


                for (Map.Entry<String, Object> entry : after.entrySet()) {
                    String fieldName = entry.getKey();
                    if (fieldName.equals("_id") || nameFieldMap.get(fieldName).getPrimaryKey() != null && nameFieldMap.get(fieldName).getPrimaryKey()) {
                        filterAfter.add(eq(entry.getKey(), entry.getValue()));
                    } else {
                        updateAfter.add(set(entry.getKey(), entry.getValue()));
                    }
                }

                mongoCollection.updateOne(and(filterAfter.toArray(new Bson[0])), Updates.combine(updateAfter.toArray(new Bson[0])), options);
                updated.incrementAndGet();
                PDKLogger.info(TAG, "Record Write TapUpdateRecordEvent {}", toJson(recordEvent));
            } else if (recordEvent instanceof TapDeleteRecordEvent) {
                if (!insertList.isEmpty()) {
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
        if (!insertList.isEmpty()) {
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
        Set<String> columnNames = connectorContext.getTable().getNameFieldMap().keySet();
        if (filters != null) {
            List<FilterResult> filterResults = new ArrayList<>();
            for (TapFilter filter : filters) {
                List<Bson> bsonList = new ArrayList<>();
                for (Map.Entry<String, Object> entry : filter.getMatch().entrySet()) {
                    bsonList.add(eq(entry.getKey(), entry.getValue()));
                }
                MongoCursor<Document> cursor = mongoCollection.find(and(bsonList.toArray(new Bson[0]))).iterator();
                FilterResult filterResult = new FilterResult();
                while (cursor.hasNext()) {
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
        if (Objects.equals(documentCount, batchReadOffset)) return 0L;
        return 10L;
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
    private void batchRead(TapConnectorContext connectorContext, Object offset, int eventBatchSize, Consumer<List<TapEvent>> tapReadOffsetConsumer) {
        initConnection(connectorContext.getConnectionConfig());
        MongoCursor<Document> mongoCursor;
        if (offset == null) {
            mongoCursor = mongoCollection.find().batchSize(eventBatchSize).iterator();
        } else {
            batchOffsetId = (ObjectId) offset;
            mongoCursor = mongoCollection.find(gt("_id", batchOffsetId)).batchSize(eventBatchSize).iterator();
        }

        Document lastDocument = null;
        while (mongoCursor.hasNext()) {
            List<TapEvent> tapEvents = list();
            for (int cnt = 1; cnt < eventBatchSize + 1; cnt++) {
                if (!mongoCursor.hasNext()) {
                    eventBatchSize = cnt;
                    break;
                }
                Map<String, Object> after = new DataMap();
                lastDocument = mongoCursor.next();
                after.putAll(lastDocument);
                TapInsertRecordEvent recordEvent = insertRecordEvent(after, connectorContext.getTable());
                tapEvents.add(recordEvent);
            }
            counter.set(counter.get() + eventBatchSize);
            if (lastDocument != null) {
                batchOffsetId = (ObjectId) lastDocument.get("_id");
                batchReadOffset = counter.get();
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
    private void streamRead(TapConnectorContext connectorContext, Object offset, Consumer<List<TapEvent>> consumer) {
        while (!isShutDown.get()) {
            List<TapEvent> tapEvents = list();
            ChangeStreamIterable<Document> changeStream;
            if (resumeToken == null) {
                changeStream = mongoCollection.watch(pipeline).fullDocument(FullDocument.UPDATE_LOOKUP);
            } else {
                changeStream = mongoCollection.watch(pipeline).resumeAfter(resumeToken).fullDocument(FullDocument.UPDATE_LOOKUP);
            }

            int maxEventsSize = 1;
            MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = changeStream.cursor();
            while (tapEvents.size() != maxEventsSize && cursor.hasNext()) {
                ChangeStreamDocument<Document> event = cursor.next();
                resumeToken = event.getResumeToken();
                OperationType operationType = event.getOperationType();
                Document fullDocument = event.getFullDocument();
                if (operationType == OperationType.INSERT) {
                    DataMap after = new DataMap();
                    after.putAll(fullDocument);
                    TapInsertRecordEvent recordEvent = insertRecordEvent(after, connectorContext.getTable());
                    tapEvents.add(recordEvent);
                } else if (operationType == OperationType.DELETE) {
                    DataMap before = new DataMap();
                    ObjectId oid = event.getDocumentKey().get("_id").asObjectId().getValue();
                    before.put("_id", oid);
                    TapDeleteRecordEvent recordEvent = deleteDMLEvent(before, connectorContext.getTable());
                    tapEvents.add(recordEvent);
                } else if (operationType == OperationType.UPDATE) {
                    DataMap before = new DataMap();
                    ObjectId oid = event.getDocumentKey().get("_id").asObjectId().getValue();
                    before.put("_id", oid);

                    DataMap after = new DataMap();
                    after.putAll(fullDocument);
                    after.remove("_id");

                    TapUpdateRecordEvent recordEvent = updateDMLEvent(before, after, connectorContext.getTable());
                    tapEvents.add(recordEvent);
                }
            }
            if (!tapEvents.isEmpty()) consumer.accept(tapEvents);
        }
    }

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
