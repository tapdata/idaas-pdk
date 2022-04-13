package io.tapdata.mongodb;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class MongoClientFactory {
    private static Map<ConnectionString, MongoClient> mongoClientMap = new ConcurrentHashMap<>();
    public static MongoClient getMongoClient(String connectionString) {
        return getMongoClient(new ConnectionString(connectionString));
    }
    public static MongoClient getMongoClient(ConnectionString connectionString) {
        return mongoClientMap.computeIfAbsent(connectionString, connectionString1 -> MongoClients.create(connectionString1));
    }

    public static void main(String... args) {
        MongoClient client = MongoClientFactory.getMongoClient(new ConnectionString("mongodb://localhost:27017/"));
        MongoClient client1 = MongoClientFactory.getMongoClient(new ConnectionString("mongodb://localhost:27017/"));
    }

}
