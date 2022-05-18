package io.tapdata.mongodb;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author jackin
 * @date 2022/5/17 20:40
 **/
public class MongodbUtil {

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
}
