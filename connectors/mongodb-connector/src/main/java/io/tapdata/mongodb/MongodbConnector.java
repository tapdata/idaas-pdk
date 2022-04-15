package io.tapdata.mongodb;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.*;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.OperationType;
import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.TapCodecRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
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
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.*;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.logger.PDKLogger;
import org.bson.*;

import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.bson.types.*;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Updates.set;
import static java.util.Collections.singletonList;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import java.io.IOException;
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
    private final int[] lock = new int[0];
    MongoCollection<Document> mongoCollection;
    private MongoOffset batchOffset = null;
    private Long documentCount = null;
    private final List<Bson> pipeline = singletonList(match(in("operationType", asList("insert", "update", "delete"))));
    private BsonDocument resumeToken = null;
    private Collection<String> primaryKeys;
    private String firstPrimaryKey;

    MongoChangeStreamCursor<ChangeStreamDocument<Document>> streamCursor;

    private void initConnection(DataMap config) throws IOException {
        mongoConfig = MongoDBConfig.load(config);
        if (mongoClient == null) {
            mongoClient = MongoClientFactory.getMongoClient(mongoConfig.getUri());
            CodecRegistry pojoCodecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
                    fromProviders(PojoCodecProvider.builder().automatic(true).build()));
            mongoDatabase = mongoClient.getDatabase(mongoConfig.getDatabase()).withCodecRegistry(pojoCodecRegistry);
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
    public void discoverSchema(TapConnectionContext connectionContext, Consumer<List<TapTable>> consumer) throws Throwable {
        initConnection(connectionContext.getConnectionConfig());
        MongoIterable<String> collectionNames = mongoDatabase.listCollectionNames();
        for (String collectionName : collectionNames) {
            consumer.accept(list(table(collectionName).defaultPrimaryKeys(singletonList("_id"))));
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
    public void connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
        initConnection(connectionContext.getConnectionConfig());
        mongoDatabase.listCollectionNames();
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
        connectorFunctions.supportQueryByAdvanceFilter(this::queryByAdvanceFilter);
        connectorFunctions.supportDropTable(this::dropTable);

        codecRegistry.registerToTapValue(ObjectId.class, value -> {
            ObjectId objValue = (ObjectId) value;
            return new TapStringValue(objValue.toHexString());
        });
        codecRegistry.registerToTapValue(Binary.class, value -> {
           Binary binary = (Binary) value;
           return new TapBinaryValue(binary.getData());
        });
        codecRegistry.registerToTapValue(Code.class, value -> {
            Code code = (Code) value;
            return new TapStringValue(code.getCode());
        });

        codecRegistry.registerToTapValue(Decimal128.class, value -> {
            Decimal128 decimal128 = (Decimal128) value;
            return new TapNumberValue(decimal128.doubleValue());
        });

        codecRegistry.registerToTapValue(Symbol.class, value -> {
            Symbol symbol = (Symbol) value;
            return new TapStringValue(symbol.getSymbol());
        });

        codecRegistry.registerFromTapValue(TapRawValue.class, "object", TapValue::getValue);
        codecRegistry.registerFromTapValue(TapArrayValue.class, "array", TapValue::getValue);
        codecRegistry.registerFromTapValue(TapMapValue.class, "object", TapValue::getValue);
        codecRegistry.registerFromTapValue(TapTimeValue.class, "date", tapTimeValue -> convertDateTimeToDate(tapTimeValue.getValue()));
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, "date", tapDateTimeValue -> convertDateTimeToDate(tapDateTimeValue.getValue()));
        codecRegistry.registerFromTapValue(TapDateValue.class, "date", tapDateValue -> convertDateTimeToDate(tapDateValue.getValue()));

        codecRegistry.registerFromTapValue(TapStringValue.class, "string", tapValue -> {
            Object originValue = tapValue.getOriginValue();
            if(originValue instanceof ObjectId) {
                return originValue;
            }
            return codecRegistry.getValueFromDefaultTapValueCodec(tapValue);
        });

        connectorFunctions.supportBatchRead(this::batchRead);
        connectorFunctions.supportBatchCount(this::batchCount);
        connectorFunctions.supportBatchOffset(this::batchOffset);
        connectorFunctions.supportStreamRead(this::streamRead);
        connectorFunctions.supportStreamOffset(this::streamOffset);
    }

    private void dropTable(TapConnectorContext connectorContext, TapDropTableEvent dropTableEvent) throws Throwable {
        initConnection(connectorContext.getConnectionConfig());
        getMongoCollection(connectorContext.getTable()).drop();

        if(streamCursor != null) {
            streamCursor.close();
            PDKLogger.info(TAG, "dropTable is called for " + connectorContext.getTable() + " streamCursor has been closed " + streamCursor);
        }
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
    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable {
        initConnection(connectorContext.getConnectionConfig());
        AtomicLong inserted = new AtomicLong(0); //insert count
        AtomicLong updated = new AtomicLong(0); //update count
        AtomicLong deleted = new AtomicLong(0); //delete count

        TapTable tapTable = connectorContext.getTable();
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        List<Document> insertList = new ArrayList<>();
        UpdateOptions options = new UpdateOptions().upsert(true);
        MongoCollection<Document> collection = getMongoCollection(connectorContext.getTable());

        for (TapRecordEvent recordEvent : tapRecordEvents) {
            if (recordEvent instanceof TapInsertRecordEvent) {
                TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) recordEvent;
                insertList.add(new Document(insertRecordEvent.getAfter()));
                inserted.incrementAndGet();
//                PDKLogger.info(TAG, "Record Write TapInsertRecordEvent {}", toJson(recordEvent));
            } else if (recordEvent instanceof TapUpdateRecordEvent) {
                if (!insertList.isEmpty()) {
                    collection.insertMany(insertList);
                }
                TapUpdateRecordEvent updateRecordEvent = (TapUpdateRecordEvent) recordEvent;
                Map<String, Object> after = updateRecordEvent.getAfter();
                Map<String, Object> before = updateRecordEvent.getBefore();


                collection.updateOne(new Document(before), new Document().append("$set", after));
                updated.incrementAndGet();
//                PDKLogger.info(TAG, "Record Write TapUpdateRecordEvent {}", toJson(recordEvent));
            } else if (recordEvent instanceof TapDeleteRecordEvent) {
                if (!insertList.isEmpty()) {
                    collection.insertMany(insertList);
                }
                TapDeleteRecordEvent deleteRecordEvent = (TapDeleteRecordEvent) recordEvent;
                Map<String, Object> before = deleteRecordEvent.getBefore();

                collection.deleteOne(new Document(before));
                deleted.incrementAndGet();
//                PDKLogger.info(TAG, "Record Write TapDeleteRecordEvent {}", toJson(recordEvent));
            }
        }
        if (!insertList.isEmpty()) {
            collection.insertMany(insertList);
        }
        //Need to tell incremental engine the write result
        writeListResultConsumer.accept(writeListResult()
                .insertedCount(inserted.get())
                .modifiedCount(updated.get())
                .removedCount(deleted.get()));
    }

    private void queryByAdvanceFilter(TapConnectorContext connectorContext, TapAdvanceFilter tapAdvanceFilter, Consumer<FilterResults> consumer) {
        MongoCollection<Document> collection = getMongoCollection(connectorContext.getTable());
        if (tapAdvanceFilter != null) {
            FilterResults filterResults = new FilterResults();
            List<Bson> bsonList = new ArrayList<>();
            DataMap match = tapAdvanceFilter.getMatch();
            if(match != null) {
                for (Map.Entry<String, Object> entry : match.entrySet()) {
                    bsonList.add(eq(entry.getKey(), entry.getValue()));
                }
            }
            List<QueryOperator> ops = tapAdvanceFilter.getOperators();
            if(ops != null) {
                for(QueryOperator op : ops) {
                    switch (op.getOperator()) {
                        case QueryOperator.GT:
                            bsonList.add(gt(op.getKey(), op.getValue()));
                            break;
                        case QueryOperator.GTE:
                            bsonList.add(gte(op.getKey(), op.getValue()));
                            break;
                        case QueryOperator.LT:
                            bsonList.add(lt(op.getKey(), op.getValue()));
                            break;
                        case QueryOperator.LTE:
                            bsonList.add(lte(op.getKey(), op.getValue()));
                            break;
                    }
                }
            }

            Integer limit = tapAdvanceFilter.getLimit();
            if(limit == null)
                limit = 1000;
            Bson query;
            if(bsonList.isEmpty())
                query = new Document();
            else
                query = and(bsonList.toArray(new Bson[0]));

            FindIterable<Document> iterable = collection.find(query).limit(limit);
            List<SortOn> sortOnList = tapAdvanceFilter.getSortOnList();
            if(sortOnList != null) {
                for(SortOn sortOn : sortOnList) {
                    switch (sortOn.getSort()) {
                        case SortOn.ASCENDING:
                            iterable.sort(Sorts.ascending(sortOn.getKey()));
                            break;
                        case SortOn.DESCENDING:
                            iterable.sort(Sorts.descending(sortOn.getKey()));
                            break;
                    }
                }
            }
            for (Document document : iterable) {
                filterResults.add(document);
            }
            consumer.accept(filterResults);
        }
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
        MongoCollection<Document> collection = getMongoCollection(connectorContext.getTable());
        if (filters != null) {
            List<FilterResult> filterResults = new ArrayList<>();
            for (TapFilter filter : filters) {
                List<Bson> bsonList = new ArrayList<>();
                for (Map.Entry<String, Object> entry : filter.getMatch().entrySet()) {
                    bsonList.add(eq(entry.getKey(), entry.getValue()));
                }
                MongoCursor<Document> cursor = collection.find(and(bsonList.toArray(new Bson[0]))).iterator();
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
    private long batchCount(TapConnectorContext connectorContext, Object offset) throws Throwable {
        initConnection(connectorContext.getConnectionConfig());
        initFirstPrimaryKey(connectorContext.getTable());

        MongoCollection<Document> collection = getMongoCollection(connectorContext.getTable());
        if (offset == null) {
            return collection.countDocuments();
        } else {
            if((offset instanceof MongoOffset)) {
                MongoOffset mongoOffset = (MongoOffset) offset;
                return collection.countDocuments(queryCondition(mongoOffset.value()));
            } else {
                long count = collection.countDocuments();
                PDKLogger.error(TAG, "Offset format is illegal {}, expect MongoOffset. Final offset will be null to do the batchCount {}", offset, count);
                return count;
            }
        }

//        if (documentCount == null) documentCount = getMongoCollection(connectorContext.getTable()).countDocuments();
//        if (Objects.equals(documentCount, batchReadOffset)) return 0L;
//        return 10L;
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
    private void batchRead(TapConnectorContext connectorContext, String offset, int eventBatchSize, Consumer<List<TapEvent>> tapReadOffsetConsumer) throws Throwable {
        initConnection(connectorContext.getConnectionConfig());
        MongoCursor<Document> mongoCursor;
        MongoCollection<Document> collection = getMongoCollection(connectorContext.getTable());
        initFirstPrimaryKey(connectorContext.getTable());

        final int batchSize = 5000;
        if (offset == null) {
            mongoCursor = collection.find().sort(Sorts.ascending(firstPrimaryKey)).batchSize(batchSize).iterator();
        } else {
            MongoOffset mongoOffset = fromJson(offset, MongoOffset.class);
            Object offsetValue = mongoOffset.value();
            if(offsetValue != null) {
                mongoCursor = collection.find(queryCondition(offsetValue)).sort(Sorts.ascending(firstPrimaryKey))
                        .batchSize(batchSize).iterator();
            } else {
                mongoCursor = collection.find().sort(Sorts.ascending(firstPrimaryKey)).batchSize(batchSize).iterator();
                PDKLogger.warn(TAG, "Offset format is illegal {}, no offset value has been found. Final offset will be null to do the batchRead", offset);
            }
        }

        Document lastDocument = null;
        List<TapEvent> tapEvents = list();
        while (mongoCursor.hasNext()) {
//            Map<String, Object> after = new DataMap();
//            after.putAll(lastDocument);
            lastDocument = mongoCursor.next();
            tapEvents.add(insertRecordEvent(lastDocument, connectorContext.getTable()));

            if(tapEvents.size() == eventBatchSize) {
                if(firstPrimaryKey != null) {
                    Object value = lastDocument.get(firstPrimaryKey);
                    batchOffset = new MongoOffset(firstPrimaryKey, value);
                }
                tapReadOffsetConsumer.accept(tapEvents);
                tapEvents = list();
            }
        }
        if(!tapEvents.isEmpty()) {
            tapReadOffsetConsumer.accept(tapEvents);
        }
//        PDKLogger.info(TAG, "batchRead Count {}", counter.get());
    }

    private Bson queryCondition(Object value) {
        return gt(firstPrimaryKey, value);
    }

    private void initFirstPrimaryKey(TapTable table) {
        if(primaryKeys == null || primaryKeys.isEmpty()) {
            primaryKeys = table.primaryKeys();
        }
        if(firstPrimaryKey == null && primaryKeys != null && !primaryKeys.isEmpty()) {
            for (String primaryKey : primaryKeys) {
                firstPrimaryKey = primaryKey;
                break;
            }
        }
    }

    private String streamOffset(TapConnectorContext connectorContext, Long offsetStartTime) {
        if(offsetStartTime != null) {
            // Unix timestamp in seconds, with increment 1
            ChangeStreamIterable<Document> changeStream = getMongoCollection(connectorContext.getTable()).watch(pipeline).fullDocument(FullDocument.UPDATE_LOOKUP);
            changeStream = changeStream.startAtOperationTime(new BsonTimestamp((int) (offsetStartTime / 1000), 1));
            MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = changeStream.cursor();
            BsonDocument theResumeToken = cursor.getResumeToken();

            if(theResumeToken != null) {
                String json =  theResumeToken.toJson();
                cursor.close();
                return json;
            }
        } else if(resumeToken != null) {
            return resumeToken.toJson();
        }
        return null;
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
    private void streamRead(TapConnectorContext connectorContext, String offset, int eventBatchSize, StreamReadConsumer consumer) {
        while (!isShutDown.get()) {
            List<TapEvent> tapEvents = list();
            ChangeStreamIterable<Document> changeStream;
            if (offset != null) {
                changeStream = getMongoCollection(connectorContext.getTable()).watch(pipeline).resumeAfter(BsonDocument.parse(offset)).fullDocument(FullDocument.UPDATE_LOOKUP);
            } else {
                changeStream = getMongoCollection(connectorContext.getTable()).watch(pipeline).fullDocument(FullDocument.UPDATE_LOOKUP);
            }

            if(streamCursor != null) {
                streamCursor.close();
            }
            streamCursor = changeStream.cursor();
            consumer.streamReadStarted();
            while (!isShutDown.get()) {
                ChangeStreamDocument<Document> event = streamCursor.tryNext();
                if(event == null) {
                    if (!tapEvents.isEmpty()) consumer.accept(tapEvents);
                    tapEvents = list();
                    boolean cursorClosed = false;
                    String errorMessage = "null";
                    try {
                        if (!streamCursor.hasNext()) {
                            cursorClosed = true;
                        }
                    } catch(Throwable throwable) {
                        throwable.printStackTrace();
                        if(throwable.getMessage().contains("closed")) {
                            cursorClosed = true;
                        }
                        errorMessage = throwable.getMessage();
                    }
                    if(cursorClosed) {
                        PDKLogger.warn(TAG, "streamCursor is not alive anymore or error occurred {}, sleep 10 seconds to avoid cpu consumption. This connector should be stopped by incremental engine.", errorMessage);
                        sleep(10000);
                    }
                    continue;
                }
                if(tapEvents.size() >= eventBatchSize) {
                    if (!tapEvents.isEmpty()) consumer.accept(tapEvents);
                    tapEvents = list();
                }

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
                    if (event.getDocumentKey() != null) {
                        before.put("_id", getIdValue(event.getDocumentKey().get("_id")));
                        TapDeleteRecordEvent recordEvent = deleteDMLEvent(before, connectorContext.getTable());
                        tapEvents.add(recordEvent);
                    } else {
                        PDKLogger.error(TAG, "Document key is null, failed to delete. {}", event);
                    }
                } else if (operationType == OperationType.UPDATE) {
                    DataMap before = new DataMap();
                    if (event.getDocumentKey() != null) {
                        before.put("_id", getIdValue(event.getDocumentKey().get("_id")));
                        DataMap after = new DataMap();
                        after.putAll(fullDocument);
                        after.remove("_id");

                        TapUpdateRecordEvent recordEvent = updateDMLEvent(before, after, connectorContext.getTable());
                        tapEvents.add(recordEvent);
                    } else {
                        PDKLogger.error(TAG, "Document key is null, failed to update. {}", event);
                    }
                }
            }

        }
    }

    private Object getIdValue(BsonValue id) {
        BsonType bsonType = id.getBsonType();
        if(bsonType == null)
            return null;
        switch (bsonType) {
//            case NULL:
//                break;
//            case ARRAY:
//                break;
            case INT32:
                return id.asInt32().getValue();
            case INT64:
                return id.asInt64().getValue();
            case BINARY:
                return id.asBinary().getData();
            case DOUBLE:
                return id.asDouble().getValue();
            case STRING:
                return id.asString().getValue();
            case SYMBOL:
                return id.asSymbol().getSymbol();
            case BOOLEAN:
                return id.asBoolean().getValue();
//            case MAX_KEY:
//                break;
//            case MIN_KEY:
//                break;
//            case DOCUMENT:
//                break;
            case DATE_TIME:
                return id.asDateTime().getValue();
            case OBJECT_ID:
                return id.asObjectId().getValue();
            case TIMESTAMP:
                return id.asTimestamp().getValue();
//            case UNDEFINED:
//                break;
//            case DB_POINTER:
//                break;
            case DECIMAL128:
                return id.asDecimal128().getValue();
//            case JAVASCRIPT:
//                break;
//            case END_OF_DOCUMENT:
//                break;
//            case REGULAR_EXPRESSION:
//                break;
//            case JAVASCRIPT_WITH_SCOPE:
//                break;
        }
        return null;
    }

    private MongoCollection<Document> getMongoCollection(TapTable table) {
        if(mongoCollection == null) {
            synchronized (lock) {
                if(mongoCollection == null) {
                    mongoCollection = mongoDatabase.getCollection(table.getName());
                }
            }
        }
        return mongoCollection;
    }

    private String batchOffset(TapConnectorContext connectorContext) throws Throwable {
        return toJson(batchOffset);
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
//        if (mongoClient != null) {
//            mongoClient.close();
//        }
        isShutDown.set(true);
        if(streamCursor != null) {
            streamCursor.close();
        }
    }
}
