import com.mongodb.client.*;
import com.mongodb.client.internal.MongoBatchCursorAdapter;
import io.tapdata.mongodb.bean.MongoDBConfig;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

@DisplayName("Bench Test")
class BasicTest {
    private MongoClient mongoClient;


    private void initConnection() throws Exception {
        if (mongoClient == null) {
            String configPath = "B:\\code\\tapdata\\idaas-pdk\\connectors\\mongodb-connector\\src\\main\\resources\\config.json";
            MongoDBConfig mongoDBConfig = MongoDBConfig.load(configPath);
            mongoClient = MongoClients.create("mongodb://" + mongoDBConfig.getHost() + ":" + mongoDBConfig.getPort());
        }
    }


    @Test
    void testConnect() {
        try {
            initConnection();
            //连接到数据库
            MongoDatabase mongoDatabase = mongoClient.getDatabase("db_example");
            MongoCollection<Document> collection = mongoDatabase.getCollection("test");
            System.out.println("document count:" + collection.countDocuments());

            MongoBatchCursorAdapter<Document> mongoCursor = (MongoBatchCursorAdapter<Document>) collection.find().batchSize(2000).iterator();
            int cnt = 1;
            int eventBatchSize = 1000;
            List<Document> documentList = new ArrayList<>();
            while (mongoCursor.hasNext()) {
                Document document = mongoCursor.next();
                documentList.add(document);
                if (mongoCursor.available() - 1 % eventBatchSize == 0) {
                    System.out.println(cnt++);
                    // 发送事件
                }
//                System.out.println(mongoCursor.next());
//                System.out.println(mongoCursor.available());
            }

            System.out.println("Connect to database successfully");
            mongoClient.close();
        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
    }

}