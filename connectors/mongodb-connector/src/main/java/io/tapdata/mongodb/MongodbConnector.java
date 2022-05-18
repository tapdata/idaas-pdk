package io.tapdata.mongodb;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.*;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.*;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;

import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.mongodb.bean.MongodbConfig;
import io.tapdata.mongodb.reader.v3.MongodbStreamReader;
import io.tapdata.mongodb.reader.MongodbV4StreamReader;
import io.tapdata.mongodb.writer.MongodbWriter;
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

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Filters.and;
import static java.util.Collections.singletonList;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Different Connector need use different "spec.json" file with different pdk id which specified in Annotation "TapConnectorClass"
 * In parent class "ConnectorBase", provides many simplified methods to develop connector
 */
@TapConnectorClass("spec.json")
public class MongodbConnector extends ConnectorBase {

		private static final String COLLECTION_ID_FIELD = "_id";
    public static final String TAG = MongodbConnector.class.getSimpleName();
    private final AtomicLong counter = new AtomicLong();
    private final AtomicBoolean isShutDown = new AtomicBoolean(false);
    private MongodbConfig mongoConfig;
    private MongoClient mongoClient;
    private MongoDatabase mongoDatabase;
    private final int[] lock = new int[0];
    MongoCollection<Document> mongoCollection;
    private MongoOffset batchOffset = null;

		private MongodbStreamReader mongodbStreamReader;

		private MongodbWriter mongodbWriter;

    private Bson queryCondition(String firstPrimaryKey, Object value) {
        return gte(firstPrimaryKey, value);
    }

    private MongoCollection<Document> getMongoCollection(String table) {
        return mongoDatabase.getCollection(table);
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
        try {
            mongoDatabase.listCollectionNames();
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY));
        } catch(Throwable throwable) {
            throwable.printStackTrace();
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, "Failed, " + throwable.getMessage()));
        }
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        MongoIterable<String> collectionNames = mongoDatabase.listCollectionNames();
        int index = 0;
        for (String collectionName : collectionNames) {
            index++;
        }
        return index;
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
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportQueryByAdvanceFilter(this::queryByAdvanceFilter);
        connectorFunctions.supportDropTable(this::dropTable);

        //Handle the special bson types, convert them to TapValue. Otherwise the unrecognized types will be converted to TapRawValue by default.
        //Target side will not easy to handle the TapRawValue.
        codecRegistry.registerToTapValue(ObjectId.class, (value, tapType) -> {
            ObjectId objValue = (ObjectId) value;
            return new TapStringValue(objValue.toHexString());
        });
        codecRegistry.registerToTapValue(Binary.class, (value, tapType) -> {
           Binary binary = (Binary) value;
           return new TapBinaryValue(binary.getData());
        });

        codecRegistry.registerToTapValue(Code.class, (value, tapType) -> {
            Code code = (Code) value;
            return new TapStringValue(code.getCode());
        });
        codecRegistry.registerToTapValue(Decimal128.class, (value, tapType) -> {
            Decimal128 decimal128 = (Decimal128) value;
            return new TapNumberValue(decimal128.doubleValue());
        });
        codecRegistry.registerToTapValue(Symbol.class, (value, tapType) -> {
            Symbol symbol = (Symbol) value;
            return new TapStringValue(symbol.getSymbol());
        });

        //TapTimeValue, TapDateTimeValue and TapDateValue's value is DateTime, need convert into Date object.
        codecRegistry.registerFromTapValue(TapTimeValue.class, "TapTime", tapTimeValue -> tapTimeValue.getValue().toDate());
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, "TapDateTime", tapDateTimeValue -> tapDateTimeValue.getValue().toDate());
        codecRegistry.registerFromTapValue(TapDateValue.class, "TapDate", tapDateValue -> tapDateValue.getValue().toDate());

        //Handle ObjectId when the source is also mongodb, we convert ObjectId to String before enter incremental engine.
        //We need check the TapStringValue, when will write to mongodb, if the originValue is ObjectId, then use originValue instead of the converted String value.
        codecRegistry.registerFromTapValue(TapStringValue.class, tapValue -> {
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
//        connectorFunctions.supportStreamOffset(this::streamOffset);
    }

    public void onStart(TapConnectionContext connectionContext) throws Throwable {
				final DataMap connectionConfig = connectionContext.getConnectionConfig();
				mongoConfig = MongodbConfig.load(connectionConfig);

				if (mongoClient == null) {
						mongoClient = MongoClients.create(mongoConfig.getUri());
						CodecRegistry pojoCodecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
										fromProviders(PojoCodecProvider.builder().automatic(true).build()));
						mongoDatabase = mongoClient.getDatabase(mongoConfig.getDatabase()).withCodecRegistry(pojoCodecRegistry);
				}
    }

    private void dropTable(TapConnectorContext connectorContext, TapDropTableEvent dropTableEvent) throws Throwable {
        getMongoCollection(dropTableEvent.getTableId()).drop();

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
    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable table, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable {
				if(mongodbWriter == null){
						mongodbWriter = new MongodbWriter();
						mongodbWriter.onStart(mongoConfig);
				}

				mongodbWriter.writeRecord(tapRecordEvents, table, writeListResultConsumer);
    }

    private void queryByAdvanceFilter(TapConnectorContext connectorContext, TapAdvanceFilter tapAdvanceFilter, TapTable table, Consumer<FilterResults> consumer) {
        MongoCollection<Document> collection = getMongoCollection(table.getId());
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
				try (final MongoCursor<Document> mongoCursor = iterable.iterator()) {
						while (mongoCursor.hasNext()) {
								filterResults.add(mongoCursor.next());
						}
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
     * @return
     */
    private long batchCount(TapConnectorContext connectorContext, TapTable table) throws Throwable {
//        MongoCollection<Document> collection = getMongoCollection(table.getId());
//        return collection.countDocuments();
        return getCollectionNotAggregateCountByTableName(mongoClient, mongoConfig.getDatabase(), table.getId(), null);
    }
    public static long getCollectionNotAggregateCountByTableName(MongoClient mongoClient, String db, String collectionName, Document filter) {
        long dbCount = 0L;
        MongoDatabase database = mongoClient.getDatabase(db);
        Document countDocument = database.runCommand(
                new Document("count", collectionName)
                        .append("query", filter == null ? new Document() : filter)
        );

        if (countDocument.containsKey("ok") && countDocument.containsKey("n")) {
            if (countDocument.get("ok").equals(1d)) {
                dbCount = Long.valueOf(countDocument.get("n") + "");
            }
        }

        return dbCount;
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
    private void batchRead(TapConnectorContext connectorContext, TapTable table, Object offset, int eventBatchSize, BiConsumer<List<TapEvent>, Object> tapReadOffsetConsumer) throws Throwable {
        List<TapEvent> tapEvents = list();
        MongoCursor<Document> mongoCursor;
        MongoCollection<Document> collection = getMongoCollection(table.getId());
        final int batchSize = eventBatchSize > 0 ? eventBatchSize : 5000;
        if (offset == null) {
            mongoCursor = collection.find().sort(Sorts.ascending(COLLECTION_ID_FIELD)).batchSize(batchSize).iterator();
        } else {
            MongoOffset mongoOffset = (MongoOffset) offset;//fromJson(offset, MongoOffset.class);
            Object offsetValue = mongoOffset.value();
            if(offsetValue != null) {
                mongoCursor = collection.find(queryCondition(COLLECTION_ID_FIELD, offsetValue)).sort(Sorts.ascending(COLLECTION_ID_FIELD))
                        .batchSize(batchSize).iterator();
            } else {
                mongoCursor = collection.find().sort(Sorts.ascending(COLLECTION_ID_FIELD)).batchSize(batchSize).iterator();
                TapLogger.warn(TAG, "Offset format is illegal {}, no offset value has been found. Final offset will be null to do the batchRead", offset);
            }
        }

        Document lastDocument = null;

        while (mongoCursor.hasNext()) {
            lastDocument = mongoCursor.next();
            tapEvents.add(insertRecordEvent(lastDocument, table.getId()));

            if(tapEvents.size() == eventBatchSize) {
								Object value = lastDocument.get(COLLECTION_ID_FIELD);
								batchOffset = new MongoOffset(COLLECTION_ID_FIELD, value);
                tapReadOffsetConsumer.accept(tapEvents, batchOffset);
                tapEvents = list();
            }
        }
        if(!tapEvents.isEmpty()) {
            tapReadOffsetConsumer.accept(tapEvents, null);
        }
    }

    private Object streamOffset(TapConnectorContext connectorContext, List<String> tableList, Long offsetStartTime) {

				if (mongodbStreamReader == null) {
						mongodbStreamReader = createStreamReader();
				}
				return mongodbStreamReader.streamOffset(tableList, offsetStartTime);
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
    private void streamRead(TapConnectorContext connectorContext, List<String> tableList, Object offset, int eventBatchSize, StreamReadConsumer consumer) {
				if (mongodbStreamReader == null){
						mongodbStreamReader = createStreamReader();
				}

				mongodbStreamReader.read(tableList, offset, eventBatchSize, consumer);

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
    public void onDestroy() {
//        if (mongoClient != null) {
//            mongoClient.close();
//        }
        isShutDown.set(true);
        if(mongodbStreamReader != null) {
						mongodbStreamReader.onDestroy();
        }

				if (mongoClient != null) {
						mongoClient.close();
				}

				if (mongodbWriter != null) {
						mongodbWriter.onDestroy();
				}
    }

		private MongodbStreamReader createStreamReader(){
				final MongodbV4StreamReader mongodb4StreamReader = new MongodbV4StreamReader();
				mongodb4StreamReader.onStart(mongoConfig);
				return mongodb4StreamReader;
		}
}
