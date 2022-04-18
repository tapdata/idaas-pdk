package io.tapdata.mongodb;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public class MongoClientFactory {
    private static Map<ConnectionString, MongoClient> mongoClientMap = new ConcurrentHashMap<>();
    public static MongoClient getMongoClient(String connectionString) {
        return getMongoClient(new ConnectionString(connectionString));
    }
    public static MongoClient getMongoClient(ConnectionString connectionString) {
        return mongoClientMap.computeIfAbsent(connectionString, connectionString1 -> {
            return MongoClients.create(connectionString1);
        });
    }

}
