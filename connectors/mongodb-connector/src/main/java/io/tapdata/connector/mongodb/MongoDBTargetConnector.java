//package io.tapdata.connector.mongodb;
//
//import com.mongodb.bulk.BulkWriteResult;
//import com.mongodb.client.MongoCollection;
//import com.mongodb.client.model.*;
//import io.tapdata.pdk.core.annotations.TargetConnectorClass;
//import io.tapdata.pdk.core.constants.DataSourceConstants;
//import io.tapdata.pdk.core.data.TransformData;
//import io.tapdata.connector.CoreTargetConnector;
//import io.tapdata.connector.mongodb.connections.MongodbConnectionFactory;
//import org.bson.Document;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//import java.util.Map;
//
//@TargetConnectorClass(DataSourceConstants.MongoDB)
//public class MongoDBTargetConnector extends CoreTargetConnector {
//    private static final Logger logger = LoggerFactory.getLogger(MongoDBTargetConnector.class.getSimpleName());
//    private MongoCollection<Document> collection;
//    @Override
//    public void verifyConnection() {
//        collection = MongodbConnectionFactory.getInstance().getCollection(targetNode);
//
//    }
//
//    @Override
//    public void init() {
//
//    }
//
//    @Override
//    public void verifyOptions() {
//
//    }
//
//    @Override
//    public void saveTransformData(List<TransformData> transformDataList) {
//        if(transformDataList == null)
//            return;
//
//        List<WriteModel<Document>> inserts = new ArrayList<>();
//
//        if(!transformDataList.isEmpty()) {
//            for(TransformData transformData : transformDataList) {
//                inserts.add(new InsertOneModel<Document>(new Document(convertDataForMongodb(transformData))));
//            }
//        }
//
//        BulkWriteResult bulkWriteResult = collection.bulkWrite(inserts, new BulkWriteOptions().ordered(false));
//        int count = bulkWriteResult.getInsertedCount() + bulkWriteResult.getModifiedCount();
//        if(count != transformDataList.size()) {
//            logger.error("loss data, expect {}, actual {}", transformDataList.size(), count);
//        }
//    }
//
//    private Map<String, Object> convertDataForMongodb(TransformData transformData) {
//        return transformData.getData();
//    }
//}
