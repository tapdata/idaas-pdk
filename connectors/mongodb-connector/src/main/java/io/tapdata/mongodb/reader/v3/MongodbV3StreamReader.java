package io.tapdata.mongodb.reader.v3;

import com.mongodb.CursorType;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.mongodb.MongodbConnector;
import io.tapdata.mongodb.MongodbUtil;
import io.tapdata.mongodb.bean.MongodbConfig;
import io.tapdata.mongodb.reader.MongodbStreamReader;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static io.tapdata.base.ConnectorBase.*;

/**
 * @author jackin
 * @date 2022/5/17 17:51
 **/
public class MongodbV3StreamReader implements MongodbStreamReader {

		public static final String TAG = MongodbConnector.class.getSimpleName();

		private final static String LOCAL_DATABASE = "local";
		private final static String OPLOG_COLLECTION = "oplog.rs";

		private MongodbConfig mongodbConfig;

		private MongoClient mongoClient;

		private Set<String> namespaces = new HashSet<>();

		private Map<String, String> nodesURI;

		private Map<String, BsonTimestamp> offset;

		private final AtomicBoolean running = new AtomicBoolean(false);

		private ThreadPoolExecutor replicaSetReadThreadPool;
		@Override
		public void onStart(MongodbConfig mongodbConfig) {
				this.mongodbConfig = mongodbConfig;
				mongoClient = MongoClients.create(mongodbConfig.getUri());
				nodesURI = MongodbUtil.nodesURI(mongoClient, mongodbConfig.getUri());
				running.compareAndSet(false, true);

				replicaSetReadThreadPool = new ThreadPoolExecutor(nodesURI.size(), nodesURI.size(), 60L, TimeUnit.SECONDS, new LinkedBlockingDeque<>());
		}

		@Override
		public void read(List<String> tableList, Object offset, int eventBatchSize, StreamReadConsumer consumer) {
				consumer.asyncMethodAndNoRetry();
				if (CollectionUtils.isNotEmpty(tableList)) {
						for (String tableName : tableList) {
								namespaces.add(new StringBuilder(mongodbConfig.getDatabase()).append(".").append(tableName).toString());
						}
				}

				if (offset != null) {
						this.offset = (Map<String, BsonTimestamp>) offset;
				}

				if (MapUtils.isNotEmpty(nodesURI)) {
						for (Map.Entry<String, String> entry : nodesURI.entrySet()) {
								final String replicaSetName = entry.getKey();
								final String mongodbURI = entry.getValue();

								replicaSetReadThreadPool.submit(() -> {
										try {
												Thread.currentThread().setName("replicaSet-read-thread-" + replicaSetName);
												readFromOplog(replicaSetName, mongodbURI, eventBatchSize, consumer);
										} catch (Exception e) {
												running.compareAndSet(true, false);
												TapLogger.error(TAG, "read oplog event from {} failed {}", replicaSetName, e.getMessage(), e);
										}
								});
						}
				}
		}

		@Override
		public void streamOffset(List<String> tableList, Long offsetStartTime, BiConsumer<Object, Long> offsetOffsetTimeConsumer) {
				if (offsetStartTime == null) {
						offsetOffsetTimeConsumer.accept(null, null);
						return;
				}
				offsetOffsetTimeConsumer.accept(new BsonTimestamp((int) (offsetStartTime / 1000), 0), null);
		}

		@Override
		public void onDestroy() {
				running.compareAndSet(true, false);
				if (mongoClient != null) {
						mongoClient.close();
				}

				if (replicaSetReadThreadPool != null) {
						replicaSetReadThreadPool.shutdownNow();

						while (!replicaSetReadThreadPool.isTerminated()) {
								TapLogger.info(TAG, "Waiting replicator thread(s) exit.");
								try {
										Thread.sleep(500l);
								} catch (InterruptedException e) {
										// nothing to do
								}
						}
				}
		}

		private void readFromOplog(String replicaSetName, String mongodbURI, int eventBatchSize, StreamReadConsumer consumer) {
				Bson filter = null;

				BsonTimestamp startTs = null;
				if (MapUtils.isNotEmpty(offset) && offset.containsKey(replicaSetName)) {
						startTs = offset.get(replicaSetName);
				} else {
						startTs = new BsonTimestamp((int) (System.currentTimeMillis() / 1000), 0);
				}

				final Bson fromMigrateFilter = Filters.exists("fromMigrate", false);

				try (MongoClient mongoclient = MongoClients.create(mongodbURI)) {

						final MongoCollection<Document> oplogCollection = mongoclient.getDatabase(LOCAL_DATABASE).getCollection(OPLOG_COLLECTION);
						List<TapEvent> tapEvents = new ArrayList<>(eventBatchSize);
						// todo exception retry
						while (running.get()) {
								filter = Filters.and(Filters.gte("ts", startTs), fromMigrateFilter);
								try (final MongoCursor<Document> mongoCursor = oplogCollection.find(filter)
												.sort(new Document("$natural", 1))
												.oplogReplay(true)
												.cursorType(CursorType.TailableAwait)
												.noCursorTimeout(true).iterator()) {

										consumer.streamReadStarted();
										while (running.get()) {
												if (mongoCursor.hasNext()) {
														final Document event = mongoCursor.next();
														final TapEvent tapEvent = handleOplogEvent(event);
														if (tapEvent == null) {
																continue;
														}
														tapEvents.add(tapEvent);

														if (tapEvents.size() % eventBatchSize == 0) {
																consumer.accept(tapEvents);
																tapEvents = new ArrayList<>();
														}
														startTs = event.get("ts", BsonTimestamp.class);
												} else {
														if (tapEvents.size() > 0) {
																consumer.accept(tapEvents);
																tapEvents = new ArrayList<>();
														}
														Thread.sleep(500L);
												}
										}
								} catch (InterruptedException e) {
										running.compareAndSet(true, false);
								}
						}
				}
		}

		protected TapEvent handleOplogEvent(Document event) {
				TapLogger.debug(TAG, "Found event: {}", event);
				String ns = event.getString("ns");
				Document object = event.get("o", Document.class);
				if (object == null) {
						TapLogger.warn(TAG, "Missing 'o' field in event, so skipping {}", event.toJson());
						return null;
				}
				TapEvent tapEvent = null;
				if (ns == null || ns.isEmpty()) {
						// These are replica set events ...
//						String msg = object.getString("msg");
//						if ("new primary".equals(msg)) {
//								AtomicReference<ServerAddress> address = new AtomicReference<>();
//								try {
//										primaryClient.executeBlocking("conn", mongoClient -> {
//												ServerAddress currentPrimary = mongoClient.getAddress();
//												address.set(currentPrimary);
//										});
//								} catch (InterruptedException e) {
//										logger.error("Get current primary executeBlocking", e);
//								}
//								ServerAddress serverAddress = address.get();
//
//								if (serverAddress != null && !serverAddress.equals(primaryAddress)) {
//										logger.info("Found new primary event in oplog, so stopping use of {} to continue with new primary",
//														primaryAddress);
//										// There is a new primary, so stop using this server and instead use the new primary ...
//										return false;
//								} else {
//										logger.info("Found new primary event in oplog, current {} is new primary. " +
//														"Continue to process oplog event.", primaryAddress);
//								}
//						}
						// Otherwise, ignore this event ...
						TapLogger.debug(TAG, "Skipping event with no namespace: {}", event.toJson());
						return null;
				}
				int delimIndex = ns.indexOf('.');
				if (delimIndex > 0) {
						String dbName = ns.substring(0, delimIndex);
						String collectionName = ns.substring(delimIndex + 1);

						// Otherwise, it is an event on a document in a collection ...
						if (!namespaces.contains(ns)) {
								TapLogger.debug(TAG, "Skipping the event for database {} based on database.whitelist");
//								try {
//										// generate last msg event's timestamp event
//										factory.recordEvent(event, clock.currentTimeInMillis(), false);
//								} catch (InterruptedException e) {
//										Thread.interrupted();
//										return false;
//								}
								return null;
						}

						if (namespaces.contains(ns)) {
								Document o = event.get("o", Document.class);
								if ("u".equalsIgnoreCase(event.getString("op"))) {
										final Document o2 = event.get("o2", Document.class);
										Object _id = o2 != null ? o2.get("_id") : o.get("_id");
										Document after = null;
										try (final MongoCursor<Document> mongoCursor = mongoClient.getDatabase(dbName).getCollection(collectionName).find(new Document("_id", _id)).iterator();) {
												if (mongoCursor.hasNext()) {
														after = mongoCursor.next();
												}
										}
										if (after == null) {
												TapLogger.warn(TAG, "Found update event _id {} already deleted in collection {}, event {}", _id, collectionName, event.toJson());
												return null;
										}
										tapEvent = updateDMLEvent(null, after, collectionName);
								} else if ("i".equalsIgnoreCase(event.getString("op"))) {
										tapEvent = insertRecordEvent(o, collectionName);
								} else if ("d".equalsIgnoreCase(event.getString("op"))) {
										tapEvent = deleteDMLEvent(o, collectionName);
								}
//								try {
//										factory.recordEvent(event, clock.currentTimeInMillis(), true);
//								} catch (InterruptedException e) {
//										Thread.interrupted();
//										return false;
//								}
						}
//						else {
//								try {
//										// generate last msg event's timestamp event
//										factory.recordEvent(event, clock.currentTimeInMillis(), false);
//								} catch (InterruptedException e) {
//										Thread.interrupted();
//										return false;
//								}
//						}
				}
				return tapEvent;
		}
}
