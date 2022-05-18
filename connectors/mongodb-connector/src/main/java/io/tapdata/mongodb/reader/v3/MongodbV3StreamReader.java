package io.tapdata.mongodb.reader.v3;

import com.mongodb.CursorType;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import io.tapdata.mongodb.MongodbUtil;
import io.tapdata.mongodb.bean.MongodbConfig;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

/**
 * @author jackin
 * @date 2022/5/17 17:51
 **/
public class MongodbV3StreamReader implements MongodbStreamReader {

		private final static String LOCAL_DATABASE = "local";
		private final static String OPLOG_COLLECTION = "rs.oplog";

		private MongodbConfig mongodbConfig;

		private MongoClient mongoClient;

		private MongoDatabase mongoDatabase;

		private Set<String> namespaces = new HashSet<>();

		private Map<String, String> nodesURI;

		private Map<String, BsonTimestamp> offset;

		private final AtomicBoolean running = new AtomicBoolean(false);
		@Override
		public void onStart(MongodbConfig mongodbConfig) {
				mongoClient = MongoClients.create(mongodbConfig.getUri());
				CodecRegistry pojoCodecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
								fromProviders(PojoCodecProvider.builder().automatic(true).build()));
				mongoDatabase = mongoClient.getDatabase(mongodbConfig.getDatabase()).withCodecRegistry(pojoCodecRegistry);

				nodesURI = MongodbUtil.nodesURI(mongoClient, mongodbConfig.getUri());

				running.compareAndSet(false, true);
		}

		@Override
		public void read(List<String> tableList, Object offset, int eventBatchSize, StreamReadConsumer consumer) {
				if (CollectionUtils.isNotEmpty(tableList)) {
						for (String tableName : tableList) {
								namespaces.add(new StringBuilder(mongodbConfig.getDatabase()).append(".").append(tableName).toString());
						}
				}

				if (offset != null) {
						this.offset = (Map<String, BsonTimestamp>) offset;
				}
		}

		@Override
		public Object streamOffset(List<String> tableList, Long offsetStartTime) {
				return null;
		}

		@Override
		public void onDestroy() {
				running.compareAndSet(true, false);
				if (mongoClient != null) {
						mongoClient.close();
				}
		}

		private void readFromOplog(String replicaSetName, String mongodbURI) {
				Bson filter = null;

				BsonTimestamp startTs = null;
				if (MapUtils.isNotEmpty(offset) && offset.containsKey(replicaSetName)) {
						startTs = offset.get(replicaSetName);
				}
				if (startTs != null) {
						filter = Filters.and(Filters.gte("ts", startTs), Filters.exists("fromMigrate", false));
				} else {
						filter = Filters.exists("fromMigrate", false);
				}


				try (MongoClient mongoclient = MongoClients.create(mongodbURI)) {

						final MongoCollection<Document> oplogCollection = mongoclient.getDatabase(LOCAL_DATABASE).getCollection(OPLOG_COLLECTION);
						try (final MongoCursor<Document> mongoCursor = oplogCollection.find(filter)
										.sort(new Document("$natural", 1))
										.oplogReplay(true)
										.cursorType(CursorType.TailableAwait)
										.noCursorTimeout(true).iterator()) {

								while (running.get() && mongoCursor.hasNext()) {
										final Document event = mongoCursor.next();

								}
						}

				}
		}
}
