package io.tapdata.connector.mongodb;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import io.tapdata.connector.mongodb.connections.MongodbConnectionFactory;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionTestResult;
import io.tapdata.pdk.apis.entity.SupportedTapEvents;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.entity.ddl.TapTableOptions;
import io.tapdata.pdk.apis.functions.consumers.TapListConsumer;
import io.tapdata.pdk.apis.logger.PDKLogger;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.apis.TapSource;
import io.tapdata.pdk.apis.TapTarget;
import io.tapdata.pdk.apis.annotations.TapConnector;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.SourceFunctions;
import io.tapdata.pdk.apis.functions.TargetFunctions;
import io.tapdata.pdk.apis.functions.consumers.TapReadOffsetConsumer;
import io.tapdata.pdk.apis.entity.TapEvent;
import io.tapdata.pdk.apis.entity.dml.TapRecordEvent;
import org.bson.*;
import org.bson.conversions.Bson;

import java.util.*;
import java.util.function.Consumer;

@TapConnector("mongo.json")
public class MongoSourceAndTarget implements TapSource, TapTarget {
    private static final String TAG = MongoSourceAndTarget.class.getSimpleName();
    MongoCollection<Document> collection;

    private TapConnectorContext connectorContext;
    @Override
    public void destroy() {
        if(connectorContext != null)
            disconnect(connectorContext);
    }

    @Override
    public void init(TapConnectorContext connectorContext, TapNodeSpecification specification) {
        this.connectorContext = connectorContext;
        connect(connectorContext);
    }

    @Override
    public void discoverSchema(TapConnectionContext databaseContext, TapListConsumer<TapTableOptions> tapListConsumer) {

    }

    @Override
    public ConnectionTestResult connectionTest(TapConnectionContext databaseContext) {
        return null;
    }

    @Override
    public void targetFunctions(TargetFunctions targetFunctions, SupportedTapEvents supportedTapEvents) {
        targetFunctions.withDMLFunction((nodeContext, recordEvents, writeConsumer) -> {
            List<WriteModel<Document>> inserts = new ArrayList<>();
            List<TapRecordEvent> insertRecords = new ArrayList<>();

            if (!recordEvents.isEmpty()) {
                for (TapRecordEvent recordEvent : recordEvents) {
                    if(recordEvent.getType().equals(TapRecordEvent.TYPE_INSERT)) {
                        inserts.add(new InsertOneModel<Document>(new Document(recordEvent.getAfter())));
                        insertRecords.add(recordEvent);
                    }
                }
            }

            BulkWriteResult bulkWriteResult = null;
            try {
                bulkWriteResult = collection.bulkWrite(inserts, new BulkWriteOptions().ordered(false));
                int count = bulkWriteResult.getInsertedCount() + bulkWriteResult.getModifiedCount();
                if (count != recordEvents.size()) {
                    PDKLogger.error(TAG, "loss data, expect {}, actual {}", recordEvents.size(), count);
                }
                writeConsumer.accept(new WriteListResult<>(recordEvents.size(), 0, 0), null);
            } catch(MongoBulkWriteException e) {
                List<BulkWriteError> errors = e.getWriteErrors();
                for(BulkWriteError error : errors) {
                    if(error.getCode() == 11000) {//E11000 duplicate key error
//                        List<WriteModel<Document>> updates = new ArrayList<>();
//                        for (TapRecordEvent recordEvent : insertRecords) {
//
//                            updates.add(new UpdateOneModel<Document>(new Document(), new Document(recordEvent.getAfter())));
//                        }
                    }
                }
            }

        });
    }

    @Override
    public void sourceFunctions(SourceFunctions sourceFunctions) {
        sourceFunctions.withBatchReadFunction(this::batchRead)
                .withStreamReadFunction(this::streamRead);

    }
    private String resumeToken;
    private void streamRead(TapConnectorContext nodeContext, Object offset, TapReadOffsetConsumer<TapEvent> consumer) {
        List<Bson> pipeline = Collections.singletonList(
                Aggregates.match(
                        Filters.in("operationType",
                                Arrays.asList("insert", "update", "replace", "delete"))));
        ChangeStreamIterable<Map> iterable = collection.watch(pipeline, Map.class);
//        iterable.resumeAfter()
        if(resumeToken != null)
            iterable.resumeAfter(BsonDocument.parse(resumeToken));
        iterable.fullDocument(FullDocument.UPDATE_LOOKUP);
        MongoChangeStreamCursor<ChangeStreamDocument<Map>> cursor = iterable.cursor();
        while(cursor.hasNext()) {
            ChangeStreamDocument<Map> changeStreamDocument = cursor.next();
            BsonDocument documentKey = changeStreamDocument.getDocumentKey();
            List<TapEvent> events = new ArrayList<>();
            BsonDocument theResumeToken = null;
            if(documentKey != null) {
                BsonValue value = documentKey.get("_id");
                if(value != null) {
                    TapRecordEvent recordEvent = new TapRecordEvent();
                    recordEvent.setTime(System.currentTimeMillis());
                    BsonTimestamp bsonTimestamp = changeStreamDocument.getClusterTime();
                    if(bsonTimestamp != null) {
                        recordEvent.setReferenceTime(bsonTimestamp.getValue());
                    }
                    switch (changeStreamDocument.getOperationType()) {
                        case INSERT:
                            recordEvent.setType(TapRecordEvent.TYPE_INSERT);
                            break;
                        case DELETE:
                            recordEvent.setType(TapRecordEvent.TYPE_DELETE);
                            break;
                        case REPLACE:
                        case UPDATE:
                            //TODO need find out whether need upsert
                            recordEvent.setType(TapRecordEvent.TYPE_UPDATE);
                            break;
                    }
                    Map document = changeStreamDocument.getFullDocument();
                    recordEvent.setAfter(document);
                    theResumeToken = changeStreamDocument.getResumeToken();
                    events.add(recordEvent);
                }
            }
            String resumeTokenStr = null;
            if(theResumeToken != null) {
                resumeTokenStr = theResumeToken.toJson();
            }
            consumer.accept(events, resumeTokenStr, null, false);
        }

    }

    private void disconnect(TapConnectorContext nodeContext) {
        Map<String, Object> connectionConfig = nodeContext.getConnectionConfig();
        if(connectionConfig == null) {
            throw new NullPointerException("Missing connection config");
        }
        String host = (String) connectionConfig.get("connectionString");
        MongodbConnectionFactory.getInstance().removeMongoClient(host);
    }

    private void connect(TapConnectorContext nodeContext) {
        Map<String, Object> connectionConfig = nodeContext.getConnectionConfig();
        if(connectionConfig == null) {
            throw new NullPointerException("Missing connection config");
        }
        String host = (String) connectionConfig.get("connectionString");
        String database = (String) connectionConfig.get("database");
        String table = nodeContext.getTable().getName();
        collection = MongodbConnectionFactory.getInstance().getCollection(host, database, table);
    }


    private void batchRead(TapConnectorContext nodeContext, Object offsetState, TapReadOffsetConsumer<TapEvent> tapReadOffsetConsumer) {
        int offset = offsetState == null ? 0 : (int) offsetState;

        int myLimit = 1000;
        FindIterable<Map> iterable = collection.find(Map.class).skip(offset).limit(myLimit);

        iterable.noCursorTimeout(true);
        MongoCursor<Map> cursor = iterable.cursor();
        List<TapEvent> recordEventList = new ArrayList<>();
        while(cursor.hasNext()) {
            Map document = cursor.next();

            TapRecordEvent recordEvent = new TapRecordEvent();
            recordEvent.setType(TapRecordEvent.TYPE_INSERT);
            recordEvent.setAfter(document);
            recordEvent.setTableName(nodeContext.getTable().getName());
            recordEvent.setTime(System.currentTimeMillis());
            recordEvent.setReferenceTime(recordEvent.getTime());
            recordEventList.add(recordEvent);
        }
        if(recordEventList.size() < myLimit) {
            tapReadOffsetConsumer.accept(recordEventList, offset + recordEventList.size(), null, true);
        } else {
            tapReadOffsetConsumer.accept(recordEventList, offset + recordEventList.size(), null, false);
        }
    }
}
