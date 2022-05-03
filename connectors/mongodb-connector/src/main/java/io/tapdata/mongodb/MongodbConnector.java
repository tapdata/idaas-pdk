package io.tapdata.mongodb;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.client.*;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.UpdateResult;
import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.codec.TapCodecRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.*;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;

import static java.util.Arrays.asList;

import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.mongodb.bean.MongoDBConfig;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.*;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.entity.logger.TapLogger;
import org.bson.*;

import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.bson.types.*;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Filters.and;
import static java.util.Collections.singletonList;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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
    //TODO need replace, deleteEvent and insertEvent.
//    private final List<Bson> pipeline = singletonList(match(in("operationType", asList("insert", "update", "delete"))));
    private BsonDocument resumeToken = null;
//    private Collection<String> primaryKeys;
//    private String firstPrimaryKey;
    private KVReadOnlyMap<TapTable> tapTableKVMap;

    MongoChangeStreamCursor<ChangeStreamDocument<Document>> streamCursor;

    private void initConnection(DataMap config) throws IOException {
        mongoConfig = MongoDBConfig.load(config);
        if (mongoClient == null) {
            //TODO watch database from MongoClientFactory.
            mongoClient = MongoClientFactory.getMongoClient(mongoConfig.getUri());
            CodecRegistry pojoCodecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
                    fromProviders(PojoCodecProvider.builder().automatic(true).build()));
            mongoDatabase = mongoClient.getDatabase(mongoConfig.getDatabase()).withCodecRegistry(pojoCodecRegistry);
        }
    }

    private Bson queryCondition(String firstPrimaryKey, Object value) {
        return gte(firstPrimaryKey, value);
    }

    //TODO support multi keys.
    private String getFirstPrimaryKey(TapTable table) {
        Collection<String> primaryKeys = table.primaryKeys();
        String firstPrimaryKey = null;
        if(primaryKeys != null && !primaryKeys.isEmpty()) {
            for (String primaryKey : primaryKeys) {
                firstPrimaryKey = primaryKey;
                break;
            }
        }
        return firstPrimaryKey;
    }

    private MongoCollection<Document> getMongoCollection(String table) {
        if(mongoCollection == null) {
            synchronized (lock) {
                if(mongoCollection == null) {
                    mongoCollection = mongoDatabase.getCollection(table);
                }
            }
        }
        return mongoCollection;
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
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        initConnection(connectionContext.getConnectionConfig());
        MongoIterable<String> collectionNames = mongoDatabase.listCollectionNames();
        //List all the tables under the database.
        List<TapTable> list = list();
        for (String collectionName : collectionNames) {
            //Mongodb is schema free. There is no way for incremental engine to know the default primary key. So need to specify the defaultPrimaryKeys.
            TapTable table;
            if(tables != null) {
                if(!tables.contains(collectionName)) {
                    continue;
                }
            }
            table = table(collectionName).defaultPrimaryKeys(singletonList("_id"));
            list.add(table);
            if(list.size() >= tableSize) {
                consumer.accept(list);
                list = list();
            }
        }
        if(!list.isEmpty())
            consumer.accept(list);
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
        try {
            mongoDatabase.listCollectionNames();
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY));
        } catch(Throwable throwable) {
            throwable.printStackTrace();
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, "Failed, " + throwable.getMessage()));
        }
    }

    /**
     * Register connector capabilities here.
     * <p>
     * To be as a target, please implement WriteRecordFunction, QueryByFilterFunction and DropTableFunction.
     * WriteRecordFunction is to write insert/update/delete events into database.
     * QueryByFilterFunction will be used to verify written record is the same with the record query from database base on the same primary keys.
     * DropTableFunction here will be used to drop the table created by tests.
     *
     * If the database need create table before record insertion, then please implement CreateTableFunction,
     * Incremental engine will generate the data types for each field base on incoming records for CreateTableFunction to create the table.
     * </p>
     *
     * <p>
     * To be as a source, please implement BatchReadFunction, BatchCountFunction, BatchOffsetFunction, StreamReadFunction and StreamOffsetFunction, QueryByAdvanceFilterFunction.
     * If the data is schema free which can not fill TapField for TapTable in discoverSchema method, Incremental Engine will sample some records to build TapField by QueryByAdvanceFilterFunction.
     * QueryByFilterFunction is not necessary, once implemented QueryByAdvanceFilterFunction.
     * BatchReadFunction is to read initial records from beginner or offset.
     * BatchCountFunction is to count initial records from beginner or offset.
     * BatchOffsetFunction is to return runtime offset during reading initial records, if batchRead not started yet, return null.
     * StreamReadFunction is to start CDC to read incremental record events, insert/update/delete.
     * StreamOffsetFunction is to return stream offset for specified timestamp or runtime stream offset.
     * </p>
     *
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
        connectorFunctions.supportInit(this::init);
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportQueryByAdvanceFilter(this::queryByAdvanceFilter);
        connectorFunctions.supportDropTable(this::dropTable);

        //Handle the special bson types, convert them to TapValue. Otherwise the unrecognized types will be converted to TapRawValue by default.
        //Target side will not easy to handle the TapRawValue.
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

        //TapTimeValue, TapDateTimeValue and TapDateValue's value is DateTime, need convert into Date object.
        codecRegistry.registerFromTapValue(TapTimeValue.class, "TapTime", tapTimeValue -> convertDateTimeToDate(tapTimeValue.getValue()));
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, "TapDateTime", tapDateTimeValue -> convertDateTimeToDate(tapDateTimeValue.getValue()));
        codecRegistry.registerFromTapValue(TapDateValue.class, "TapDate", tapDateValue -> convertDateTimeToDate(tapDateValue.getValue()));

        //Handle ObjectId when the source is also mongodb, we convert ObjectId to String before enter incremental engine.
        //We need check the TapStringValue, when will write to mongodb, if the originValue is ObjectId, then use originValue instead of the converted String value.
        codecRegistry.registerFromTapValue(TapStringValue.class, "string", tapValue -> {
            Object originValue = tapValue.getOriginValue();
            if(originValue instanceof ObjectId) {
                return originValue;
            }
            //If not ObjectId, use default TapValue Codec to convert.
            return codecRegistry.getValueFromDefaultTapValueCodec(tapValue);
        });

        //TO be as a source, need to implement below methods.
        connectorFunctions.supportBatchRead(this::batchRead);
        connectorFunctions.supportBatchCount(this::batchCount);
        connectorFunctions.supportStreamRead(this::streamRead);
        connectorFunctions.supportStreamOffset(this::streamOffset);
    }

    private void init(TapConnectorContext connectorContext, KVReadOnlyMap<TapTable> tapTableKVMap) throws Throwable {
        this.tapTableKVMap = tapTableKVMap;
        initConnection(connectorContext.getConnectionConfig());
    }

    private void dropTable(TapConnectorContext connectorContext, TapDropTableEvent dropTableEvent) throws Throwable {
        initConnection(connectorContext.getConnectionConfig());
        getMongoCollection(dropTableEvent.getTableId()).drop();

        if(streamCursor != null) {
            streamCursor.close();
            TapLogger.info(TAG, "dropTable is called for " + dropTableEvent.getTableId() + " streamCursor has been closed " + streamCursor);
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
     *  if(needCreateTable)
     *      createTable
     *  if(needClearTable)
     *      clearTable
     *  if(needDropTable)
     *      dropTable
     *  writeRecord
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

        Map<String, List<Document>> insertMap = new HashMap<>();
        Map<String, List<TapRecordEvent>> insertEventMap = new HashMap<>();
        UpdateOptions options = new UpdateOptions().upsert(true);

        WriteListResult<TapRecordEvent> writeListResult = writeListResult();

        for (TapRecordEvent recordEvent : tapRecordEvents) {
            MongoCollection<Document> collection = getMongoCollection(recordEvent.getTableId());

            if (recordEvent instanceof TapInsertRecordEvent) {
                TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) recordEvent;
                insertMap.computeIfAbsent(insertRecordEvent.getTableId(), s -> new ArrayList<>()).add(new Document(insertRecordEvent.getAfter()));
//                insertMap.put(insertRecordEvent.getTableName(), new Document(insertRecordEvent.getAfter()));
                insertEventMap.computeIfAbsent(insertRecordEvent.getTableId(), s -> new ArrayList<>()).add(insertRecordEvent);
                inserted.incrementAndGet();
            } else if (recordEvent instanceof TapUpdateRecordEvent) {
                if (!insertMap.isEmpty()) {
                    insertMany(insertMap, insertEventMap, writeListResult);
                }
                TapUpdateRecordEvent updateRecordEvent = (TapUpdateRecordEvent) recordEvent;
                Map<String, Object> after = updateRecordEvent.getAfter();
                Map<String, Object> before = updateRecordEvent.getBefore();


                UpdateResult updateResult = collection.updateOne(new Document(before), new Document().append("$set", after), options);
                if(updateResult.getModifiedCount() > 0)
                    updated.incrementAndGet();
                else
                    TapLogger.warn("Update record by filter {} is missed. ", toJson(before));
            } else if (recordEvent instanceof TapDeleteRecordEvent) {
                if (!insertMap.isEmpty()) {
                    insertMany(insertMap, insertEventMap, writeListResult);
                }
                TapDeleteRecordEvent deleteRecordEvent = (TapDeleteRecordEvent) recordEvent;
                Map<String, Object> before = deleteRecordEvent.getBefore();

                DeleteResult deleteResult = collection.deleteOne(new Document(before));
                if(deleteResult.getDeletedCount() > 0)
                    deleted.incrementAndGet();
                else
                    TapLogger.warn(TAG, "Delete record by filter {} is missed. ", toJson(before));
            }
        }
        if (!insertMap.isEmpty()) {
            insertMany(insertMap, insertEventMap, writeListResult);
        }
        //Need to tell incremental engine the write result
        writeListResultConsumer.accept(writeListResult
                .insertedCount(inserted.get())
                .modifiedCount(updated.get())
                .removedCount(deleted.get()));
    }

    private void insertMany(Map<String, List<Document>> insertMap, Map<String, List<TapRecordEvent>> insertEventMap, WriteListResult<TapRecordEvent> writeListResult) {
        for(Map.Entry<String, List<Document>> entry : insertMap.entrySet()) {
            List<TapRecordEvent> insertEventList = insertEventMap.get(entry.getKey());
            InsertManyResult insertManyResult = getMongoCollection(entry.getKey()).insertMany(entry.getValue());
            Map<Integer, BsonValue> insertedIds = insertManyResult.getInsertedIds();
            Exception error = new Exception("Insert failed");
            //Tell incremental engine which event insert failed. incremental engine will try update event.
            for (int i = 0; i < insertMap.size(); i++) {
                if(!insertedIds.containsKey(i))
                    writeListResult.addError(insertEventList.get(i), error);
            }
        }

    }

    private void queryByAdvanceFilter(TapConnectorContext connectorContext, TapAdvanceFilter tapAdvanceFilter, Consumer<FilterResults> consumer) {
        MongoCollection<Document> collection = getMongoCollection(tapAdvanceFilter.getTableId());
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

        Integer skip = tapAdvanceFilter.getSkip();
        if(skip != null) {
            iterable.skip(skip);
        }

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
    private long batchCount(TapConnectorContext connectorContext, TapTable table, String offset) throws Throwable {
        initConnection(connectorContext.getConnectionConfig());
        String firstPrimaryKey = getFirstPrimaryKey(table);

        MongoCollection<Document> collection = getMongoCollection(table.getId());
        if (offset == null) {
            return collection.countDocuments();
        } else {
            MongoOffset mongoOffset = fromJson(offset, MongoOffset.class);
            return collection.countDocuments(queryCondition(firstPrimaryKey, mongoOffset.value()));
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
     * @param tapReadOffsetConsumer
     */
    private void batchRead(TapConnectorContext connectorContext, TapTable table, String offset, int eventBatchSize, BiConsumer<List<TapEvent>, String> tapReadOffsetConsumer) throws Throwable {
        List<TapEvent> tapEvents = list();
        MongoCursor<Document> mongoCursor;
        MongoCollection<Document> collection = getMongoCollection(table.getId());
        String firstPrimaryKey = getFirstPrimaryKey(table);
        //TODO sort multi primary keys, close to exactly once.
        final int batchSize = 5000;
        if (offset == null) {
            mongoCursor = collection.find().sort(Sorts.ascending(firstPrimaryKey)).batchSize(batchSize).iterator();
        } else {
            MongoOffset mongoOffset = fromJson(offset, MongoOffset.class);
            Object offsetValue = mongoOffset.value();
            if(offsetValue != null) {
                mongoCursor = collection.find(queryCondition(firstPrimaryKey, offsetValue)).sort(Sorts.ascending(firstPrimaryKey))
                        .batchSize(batchSize).iterator();
            } else {
                mongoCursor = collection.find().sort(Sorts.ascending(firstPrimaryKey)).batchSize(batchSize).iterator();
                TapLogger.warn(TAG, "Offset format is illegal {}, no offset value has been found. Final offset will be null to do the batchRead", offset);
            }
        }

        Document lastDocument = null;

        while (mongoCursor.hasNext()) {
            lastDocument = mongoCursor.next();
            tapEvents.add(insertRecordEvent(lastDocument, table.getId()));

            if(tapEvents.size() == eventBatchSize) {
                if(firstPrimaryKey != null) {
                    Object value = lastDocument.get(firstPrimaryKey);
                    batchOffset = new MongoOffset(firstPrimaryKey, value);
                }
                tapReadOffsetConsumer.accept(tapEvents, toJson(batchOffset));
                tapEvents = list();
            }
        }
        if(!tapEvents.isEmpty()) {
            tapReadOffsetConsumer.accept(tapEvents, null);
        }
    }

    private String streamOffset(TapConnectorContext connectorContext, List<String> tableList, Long offsetStartTime) {
        if(offsetStartTime != null) {
            List<Bson> pipeline = singletonList(Aggregates.match(
                    Filters.in("ns.coll", tableList)
            ));
            // Unix timestamp in seconds, with increment 1
            ChangeStreamIterable<Document> changeStream = mongoDatabase.watch(pipeline).fullDocument(FullDocument.UPDATE_LOOKUP);
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
    private void streamRead(TapConnectorContext connectorContext, List<String> tableList, String offset, int eventBatchSize, StreamReadConsumer consumer) {
        List<Bson> pipeline = singletonList(Aggregates.match(
                Filters.in("ns.coll", tableList)
        ));
//        pipeline = new ArrayList<>();
//        List<Bson> collList = tableList.stream().map(t -> Filters.eq("ns.coll", t)).collect(Collectors.toList());
//        List<Bson> pipeline1 = asList(Aggregates.match(Filters.or(collList)));

        while (!isShutDown.get()) {
            List<TapEvent> tapEvents = list();
            ChangeStreamIterable<Document> changeStream;
            if (offset != null) {
                changeStream = mongoDatabase.watch(pipeline).resumeAfter(BsonDocument.parse(offset)).fullDocument(FullDocument.UPDATE_LOOKUP);
            } else {
                changeStream = mongoDatabase.watch(pipeline).fullDocument(FullDocument.UPDATE_LOOKUP);
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
                        TapLogger.warn(TAG, "streamCursor is not alive anymore or error occurred {}, sleep 10 seconds to avoid cpu consumption. This connector should be stopped by incremental engine.", errorMessage);
                        sleep(10000);
                    }
                    continue;
                }
                if(tapEvents.size() >= eventBatchSize) {
                    if (!tapEvents.isEmpty()) consumer.accept(tapEvents);
                    tapEvents = list();
                }

                MongoNamespace mongoNamespace = event.getNamespace();

                String collectionName = null;
                if(mongoNamespace != null) {
                    collectionName = mongoNamespace.getCollectionName();
                }
                if(collectionName == null)
                    continue;
                resumeToken = event.getResumeToken();
                OperationType operationType = event.getOperationType();
                Document fullDocument = event.getFullDocument();
                if (operationType == OperationType.INSERT) {
                    DataMap after = new DataMap();
                    after.putAll(fullDocument);
                    TapInsertRecordEvent recordEvent = insertRecordEvent(after, collectionName);
                    tapEvents.add(recordEvent);
                } else if (operationType == OperationType.DELETE) {
                    DataMap before = new DataMap();
                    if (event.getDocumentKey() != null) {
                        before.put("_id", getIdValue(event.getDocumentKey().get("_id")));
                        TapDeleteRecordEvent recordEvent = deleteDMLEvent(before, collectionName);
                        tapEvents.add(recordEvent);
                    } else {
                        TapLogger.error(TAG, "Document key is null, failed to delete. {}", event);
                    }
                } else if (operationType == OperationType.UPDATE) {
                    DataMap before = new DataMap();
                    if (event.getDocumentKey() != null) {
                        before.put("_id", getIdValue(event.getDocumentKey().get("_id")));
                        DataMap after = new DataMap();
                        after.putAll(fullDocument);
                        after.remove("_id");

                        TapUpdateRecordEvent recordEvent = updateDMLEvent(before, after, collectionName);
                        tapEvents.add(recordEvent);
                    } else {
                        TapLogger.error(TAG, "Document key is null, failed to update. {}", event);
                    }
                } else {
                    TapLogger.error(TAG, "Unsupported operationType {}, {}", operationType, event);
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
