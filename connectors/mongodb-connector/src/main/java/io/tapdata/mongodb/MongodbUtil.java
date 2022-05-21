package io.tapdata.mongodb;

import com.mongodb.ConnectionString;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import io.tapdata.entity.logger.TapLogger;
import org.apache.commons.lang3.StringUtils;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.IntStream;

/**
 * @author jackin
 * @date 2022/5/17 20:40
 **/
public class MongodbUtil {

		private static final int SAMPLE_SIZE_BATCH_SIZE = 1000;

		private final static String BUILDINFO = "buildinfo";
		private final static String VERSION = "version";

		public static int getVersion(MongoClient mongoClient, String database) {
				int versionNum = 0;
				MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
				Document buildinfo = mongoDatabase.runCommand(new BsonDocument(BUILDINFO, new BsonString("")));
				String versionStr = buildinfo.get(VERSION).toString();
				String[] versions = versionStr.split("\\.");
				versionNum = Integer.valueOf(versions[0]);

				return versionNum;
		}

		public static String getVersionString(MongoClient mongoClient, String database) {
				MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
				Document buildinfo = mongoDatabase.runCommand(new BsonDocument(BUILDINFO, new BsonString("")));
				return buildinfo.get(VERSION).toString();
		}

		public static void sampleDataRow(MongoCollection collection, int sampleSize, Consumer<BsonDocument> callback) {

				int sampleTime = 1;
				int sampleBatchSize = SAMPLE_SIZE_BATCH_SIZE;
				if (sampleSize > SAMPLE_SIZE_BATCH_SIZE) {
						if (sampleSize % SAMPLE_SIZE_BATCH_SIZE != 0) {
								sampleTime = sampleSize / SAMPLE_SIZE_BATCH_SIZE + 1;
						} else {
								sampleTime = sampleSize / SAMPLE_SIZE_BATCH_SIZE;
						}
				} else {
						sampleBatchSize = sampleSize;
				}
				int finalSampleBatchSize = sampleBatchSize;
				IntStream.range(0, sampleTime).forEach(i -> {
						List<Document> pipeline = new ArrayList<>();
						pipeline.add(new Document("$sample", new Document("size", finalSampleBatchSize)));
						try (MongoCursor<BsonDocument> cursor = collection.aggregate(pipeline, BsonDocument.class).allowDiskUse(true).iterator()){
								while (cursor.hasNext()) {
										BsonDocument next = cursor.next();
										callback.accept(next);
								}
						} catch (Exception e) {
								e.printStackTrace();
						}
				});
		}

		public static Map<String, String> nodesURI(MongoClient mongoClient, String mongodbURI){
				Map<String, String> nodeConnURIs = new HashMap<>();
				ConnectionString connectionString = new ConnectionString(mongodbURI);
				String username = connectionString.getUsername();
				String password = connectionString.getPassword() != null && connectionString.getPassword().length > 0 ? new String(connectionString.getPassword()) : null;
				final String database = connectionString.getDatabase();
				final String mongoDBURIOptions = getMongoDBURIOptions(mongodbURI);
				MongoCollection<Document> collection = mongoClient.getDatabase("config").getCollection("shards");
				final MongoCursor<Document> cursor = collection.find().iterator();
				while (cursor.hasNext()) {
						Document doc = cursor.next();
						String hostStr = doc.getString("host");
						String replicaSetName = replicaSetUsedIn(hostStr);
						String addresses = hostStr.split("/")[1];
						StringBuilder sb = new StringBuilder();
						if (StringUtils.isNotEmpty(username) && StringUtils.isNotEmpty(password)) {
								try {
										sb.append("mongodb://").append(URLEncoder.encode(connectionString.getUsername(), "UTF-8")).append(":").append(URLEncoder.encode(String.valueOf(password), "UTF-8")).append("@").append(addresses).append("/").append(database);
								} catch (UnsupportedEncodingException e) {
										throw new RuntimeException("url encode username/password failed", e);
								}
						} else {
								sb.append("mongodb://").append(addresses).append("/").append(database);
						}
						if (StringUtils.isNotBlank(mongoDBURIOptions)) {
								sb.append("?").append(mongoDBURIOptions);
						}
						nodeConnURIs.put(replicaSetName, sb.toString());
				}

				if (nodeConnURIs.size() == 0) {
						// The addresses may be a replica set ...
						try {
								Document document = mongoClient.getDatabase("admin").runCommand(new Document("replSetGetStatus", 1));
								List members = document.get("members", List.class);
								if (members != null && !members.isEmpty()) {

										StringBuilder sb = new StringBuilder();
										// This is a replica set ...
										for (Object member : members) {
												Document doc = (Document) member;
												sb.append(doc.getString("name")).append(",");
										}
										String addressStr = sb.deleteCharAt(sb.length() - 1).toString();
										String replicaSetName = document.getString("set");

										StringBuilder uriSB = new StringBuilder();
										if (StringUtils.isNotBlank(username) && StringUtils.isNotBlank(password)) {
												uriSB.append("mongodb://").append(URLEncoder.encode(username, "UTF-8")).append(":").append(URLEncoder.encode(password, "UTF-8")).append("@").append(addressStr).append("/").append(database);
										} else {
												uriSB.append("mongodb://").append(addressStr).append("/").append(database);
										}
										if (StringUtils.isNotBlank(mongoDBURIOptions)) {
												uriSB.append("?").append(mongoDBURIOptions);
										}
										nodeConnURIs.put(replicaSetName, uriSB.toString());
								}
						} catch (Exception e) {
								String replicaSetName = "single";
								if (replicaSetName != null) {

										for (String address : connectionString.getHosts()) {
												StringBuilder sb = new StringBuilder();
												if (StringUtils.isNotBlank(username) && StringUtils.isNotBlank(password)) {
														try {
																sb.append("mongodb://").append(URLEncoder.encode(username, "UTF-8")).append(":").append(URLEncoder.encode(password, "UTF-8")).append("@").append(address).append("/").append(database);
														} catch (UnsupportedEncodingException ex) {
																throw new RuntimeException("url encode username/password failed", e);
														}
												} else {
														sb.append("mongodb://").append(address).append("/").append(database);
												}
												if (StringUtils.isNotBlank(mongoDBURIOptions)) {
														sb.append("?").append(mongoDBURIOptions);
												}
												nodeConnURIs.put(replicaSetName, sb.toString());
										}
								}
						}
				}

				return nodeConnURIs;
		}

		public static String replicaSetUsedIn(String addresses) {
				if (addresses.startsWith("[")) {
						// Just an IPv6 address, so no replica set name ...
						return null;
				}
				// Either a replica set name + an address, or just an IPv4 address ...
				int index = addresses.indexOf('/');
				if (index < 0) return null;
				return addresses.substring(0, index);
		}

		public static String getMongoDBURIOptions(String databaseUri) {
				String options = null;
				try {

						if (StringUtils.isNotBlank(databaseUri)) {
								String[] split = databaseUri.split("\\?", 2);
								if (split.length == 2) {
										options = split[1];
								}
						}

				} catch (Exception e) {
						throw new RuntimeException(e);
				}

				return options;
		}

		public static String maskUriPassword(String mongodbUri) {
				if (StringUtils.isNotBlank(mongodbUri)) {
						try {
								ConnectionString connectionString = new ConnectionString(mongodbUri);
								MongoCredential credentials = connectionString.getCredential();
								if (credentials != null) {
										char[] password = credentials.getPassword();
										if (password != null) {
												String pass = new String(password);
												pass = URLEncoder.encode(pass, "UTF-8");

												mongodbUri = StringUtils.replaceOnce(mongodbUri, pass + "@", "******@");
										}
								}

						} catch (Exception e) {
								TapLogger.error(MongodbUtil.class.getSimpleName(), "Mask password for mongodb uri {} failed {}", mongodbUri, e);
						}
				}

				return mongodbUri;
		}
}
