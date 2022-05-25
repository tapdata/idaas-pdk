package io.tapdata.mongodb.writer;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.mongodb.bean.MongodbConfig;
import io.tapdata.mongodb.reader.MongodbV4StreamReader;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.writeListResult;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

/**
 * @author jackin
 * @date 2022/5/17 18:30
 **/
public class MongodbWriter {

		public static final String TAG = MongodbV4StreamReader.class.getSimpleName();

		private MongoClient mongoClient;
		private MongoDatabase mongoDatabase;

		public void onStart(MongodbConfig mongodbConfig) {
				if (mongoClient == null) {
						//TODO watch database from MongoClientFactory.
						mongoClient = MongoClients.create(mongodbConfig.getUri());
						CodecRegistry pojoCodecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
										fromProviders(PojoCodecProvider.builder().automatic(true).build()));
						mongoDatabase = mongoClient.getDatabase(mongodbConfig.getDatabase()).withCodecRegistry(pojoCodecRegistry);
				}
		}

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
		 * @param tapRecordEvents
		 * @param writeListResultConsumer
		 */
		public void writeRecord(List<TapRecordEvent> tapRecordEvents, TapTable table, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable {
				AtomicLong inserted = new AtomicLong(0); //insert count
				AtomicLong updated = new AtomicLong(0); //update count
				AtomicLong deleted = new AtomicLong(0); //delete count

				List<WriteModel<Document>> writeModels = new ArrayList<>();
//				Map<String, List<Document>> insertMap = new HashMap<>();
//				Map<String, List<TapRecordEvent>> insertEventMap = new HashMap<>();
				UpdateOptions options = new UpdateOptions().upsert(true);

				WriteListResult<TapRecordEvent> writeListResult = writeListResult();

				MongoCollection<Document> collection = getMongoCollection(table.getId());

				final Collection<String> pks = table.primaryKeys();
				for (TapRecordEvent recordEvent : tapRecordEvents) {
						if (recordEvent instanceof TapInsertRecordEvent) {
								TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) recordEvent;
								final Document pkFilter = getPkFilter(pks, insertRecordEvent.getAfter());
//								writeModels.add(new InsertOneModel<>(new Document(insertRecordEvent.getAfter())));
								writeModels.add(new UpdateManyModel<>(pkFilter, new Document().append("$set", insertRecordEvent.getAfter()), options));
								inserted.incrementAndGet();
						} else if (recordEvent instanceof TapUpdateRecordEvent) {

								TapUpdateRecordEvent updateRecordEvent = (TapUpdateRecordEvent) recordEvent;
								Map<String, Object> after = updateRecordEvent.getAfter();
								Map<String, Object> before = updateRecordEvent.getBefore();
								final Document pkFilter = getPkFilter(pks, before != null && !before.isEmpty() ? before : after);

								writeModels.add(new UpdateManyModel<>(pkFilter, new Document().append("$set", after), options));

						} else if (recordEvent instanceof TapDeleteRecordEvent) {

								TapDeleteRecordEvent deleteRecordEvent = (TapDeleteRecordEvent) recordEvent;
								Map<String, Object> before = deleteRecordEvent.getBefore();
								final Document pkFilter = getPkFilter(pks, before);

								writeModels.add(new DeleteOneModel<>(pkFilter));
								collection.deleteOne(new Document(before));
								deleted.incrementAndGet();
						}
				}

				final MongoCollection<Document> mongoCollection = getMongoCollection(table.getId());
				mongoCollection.bulkWrite(writeModels, new BulkWriteOptions().ordered(true));
				//Need to tell incremental engine the write result
				writeListResultConsumer.accept(writeListResult
								.insertedCount(inserted.get())
								.modifiedCount(updated.get())
								.removedCount(deleted.get()));
		}

		public void onDestroy(){
				if (mongoClient != null) {
						mongoClient.close();
				}
		}

		private MongoCollection<Document> getMongoCollection(String table) {
				return mongoDatabase.getCollection(table);
		}

		private Document getPkFilter(Collection<String> pks, Map<String, Object> record) {
				Document filter = new Document();
				for (String pk : pks) {
						filter.append(pk, record.get(pk));
				}

				return filter;
		}
}
