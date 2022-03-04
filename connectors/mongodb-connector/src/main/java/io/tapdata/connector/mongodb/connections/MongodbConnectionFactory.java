package io.tapdata.connector.mongodb.connections;

import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.MapCodec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MongodbConnectionFactory {
    private static final MongodbConnectionFactory instance = new MongodbConnectionFactory();

    private Map<String, MongoClient> connectionClientMap = new ConcurrentHashMap<>();

    public static MongodbConnectionFactory getInstance() {
        return instance;
    }

    public MongoClient getMongoClient(String connectionString) {
        return connectionClientMap.computeIfAbsent(connectionString, MongoClients::create);
    }

    public MongoClient removeMongoClient(String connectionString) {
        MongoClient mongoClient = connectionClientMap.remove(connectionString);
        if(mongoClient != null) {
            mongoClient.close();
        }
        return mongoClient;
    }

    public MongoCollection<Document> getCollection(String connectionString, String databaseName, String table) {
        MongoDatabase database;
        MongoClient mongoClient = MongodbConnectionFactory.getInstance().getMongoClient(connectionString);
//        CodecRegistry codecRegistry = CodecRegistries.fromRegistries(
//                CodecRegistries.fromCodecs(new MapCodec()),
//                CodecRegistries.fromProviders(new CodecProvider() {
//                    @Override
//                    public <T> Codec<T> get(Class<T> clazz, CodecRegistry registry) {
//                        return null;
//                    }
//                }),
//                MongoClientSettings.getDefaultCodecRegistry());
//        CodecRegistry pojoCodecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
//                fromProviders(PojoCodecProvider.builder().automatic(true).build()));
        database = mongoClient.getDatabase(databaseName)
//                .withCodecRegistry(pojoCodecRegistry)
                .withWriteConcern(WriteConcern.JOURNALED)//写策略
                .withReadConcern(ReadConcern.MAJORITY)//读策略：只能读到成功写入大多数节点的数据（所以有可能读到旧的数据）
                .withReadPreference(ReadPreference.nearest());//读选取节点策略：网络最近

        return database.getCollection(table);
    }
}
