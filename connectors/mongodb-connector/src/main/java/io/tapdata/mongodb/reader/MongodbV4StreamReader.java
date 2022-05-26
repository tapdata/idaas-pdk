package io.tapdata.mongodb.reader;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.client.*;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.OperationType;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.mongodb.MongodbUtil;
import io.tapdata.mongodb.bean.MongodbConfig;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import org.apache.commons.collections4.MapUtils;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;

import javax.print.Doc;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static io.tapdata.base.ConnectorBase.*;
import static java.util.Collections.singletonList;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

/**
 * @author jackin
 * @date 2022/5/17 14:34
 **/
public class MongodbV4StreamReader implements MongodbStreamReader {

		public static final String TAG = MongodbV4StreamReader.class.getSimpleName();

		private BsonDocument resumeToken = null;
		private final AtomicBoolean running = new AtomicBoolean(false);
		private MongoClient mongoClient;
		private MongoDatabase mongoDatabase;
		private MongodbConfig mongodbConfig;

		@Override
		public void onStart(MongodbConfig mongodbConfig) {
				this.mongodbConfig = mongodbConfig;
				if (mongoClient == null) {
						mongoClient = MongoClients.create(mongodbConfig.getUri());
						CodecRegistry pojoCodecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
										fromProviders(PojoCodecProvider.builder().automatic(true).build()));
						mongoDatabase = mongoClient.getDatabase(mongodbConfig.getDatabase()).withCodecRegistry(pojoCodecRegistry);
				}
		}

		@Override
		public void read(List<String> tableList, Object offset, int eventBatchSize, StreamReadConsumer consumer){
				List<Bson> pipeline = singletonList(Aggregates.match(
								Filters.in("ns.coll", tableList)
				));
//        pipeline = new ArrayList<>();
//        List<Bson> collList = tableList.stream().map(t -> Filters.eq("ns.coll", t)).collect(Collectors.toList());
//        List<Bson> pipeline1 = asList(Aggregates.match(Filters.or(collList)));

				while (!running.get()) {
						List<TapEvent> tapEvents = list();
						ChangeStreamIterable<Document> changeStream;
						if (offset != null) {
								//报错之后， 再watch一遍
								//如果完全没事件， 就需要从当前时间开始watch
								if (offset instanceof BsonTimestamp) {
										changeStream = mongoDatabase.watch(pipeline).startAtOperationTime((BsonTimestamp) offset).fullDocument(FullDocument.UPDATE_LOOKUP);
								} else {
										changeStream = mongoDatabase.watch(pipeline).resumeAfter((BsonDocument) offset).fullDocument(FullDocument.UPDATE_LOOKUP);
								}
						} else {
								changeStream = mongoDatabase.watch(pipeline).fullDocument(FullDocument.UPDATE_LOOKUP);
						}
						final MongoChangeStreamCursor<ChangeStreamDocument<Document>> streamCursor = changeStream.cursor();
						consumer.streamReadStarted();
						while (!running.get()) {
								try {
										ChangeStreamDocument<Document> event = streamCursor.tryNext();
										if(event == null) {
												if (!tapEvents.isEmpty()) {
														consumer.accept(tapEvents, resumeToken);
														tapEvents = list();
												}
												continue;
										}
										if(tapEvents.size() >= eventBatchSize) {
												consumer.accept(tapEvents, resumeToken);
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
												recordEvent.setReferenceTime((long) (event.getClusterTime().getTime()) * 1000);
												tapEvents.add(recordEvent);
										} else if (operationType == OperationType.DELETE) {
												DataMap before = new DataMap();
												if (event.getDocumentKey() != null) {
														final Document documentKey = new DocumentCodec().decode(new BsonDocumentReader(event.getDocumentKey()), DecoderContext.builder().build());
														before.put("_id", documentKey.get("_id"));
														TapDeleteRecordEvent recordEvent = deleteDMLEvent(before, collectionName);
														recordEvent.setReferenceTime((long) (event.getClusterTime().getTime()) * 1000);
														tapEvents.add(recordEvent);
												} else {
														TapLogger.warn(TAG, "Document key is null, failed to delete. {}", event);
												}
										} else if (operationType == OperationType.UPDATE) {
												DataMap before = new DataMap();
												DataMap after = new DataMap();
												if (MapUtils.isEmpty(fullDocument)) {
														TapLogger.warn(TAG, "Found update event already deleted in collection %s, _id %s", collectionName, event.getDocumentKey().get("_id"));
														continue;
												}
												if (event.getDocumentKey() != null) {
														before.put("_id", fullDocument.get("_id"));
														after.putAll(fullDocument);

														TapUpdateRecordEvent recordEvent = updateDMLEvent(before, after, collectionName);
														recordEvent.setReferenceTime((long) (event.getClusterTime().getTime()) * 1000);
														tapEvents.add(recordEvent);
												} else {
														TapLogger.error(TAG, "Document key is null, failed to update. {}", event);
												}
										} else {
												TapLogger.error(TAG, "Unsupported operationType {}, {}", operationType, event);
										}
								} catch (Throwable throwable) {
										if (!running.get() && throwable instanceof IllegalStateException) {
												if (throwable.getMessage().contains("state should be: open") || throwable.getMessage().contains("Cursor has been closed")) {
												}
												return;
										}
										TapLogger.error(TAG, "Read change stream from {}, failed {}", MongodbUtil.maskUriPassword(mongodbConfig.getUri()), throwable.getMessage(), throwable);
										throw throwable;
								}
						}
				}
		}

		@Override
		public Object streamOffset(Long offsetStartTime) {
				Object offset = null;
				ChangeStreamIterable<Document> changeStream = mongoDatabase.watch();
				if (offsetStartTime != null) {
						changeStream = changeStream.startAtOperationTime(new BsonTimestamp((int) (offsetStartTime / 1000), 0));
						try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = changeStream.cursor();) {
								BsonDocument theResumeToken = cursor.getResumeToken();

								if (theResumeToken != null) {
										offset = theResumeToken;
								}
						}
				}

				if (offset == null) {
						final long serverTimestamp = MongodbUtil.mongodbServerTimestamp(mongoDatabase);
						offset = new BsonTimestamp((int) (serverTimestamp / 1000), 0);
				}

				return offset;
		}

		@Override
		public void onDestroy() {
				running.compareAndSet(true, false);
				if (mongoClient != null) {
						mongoClient.close();
				}
		}
}
