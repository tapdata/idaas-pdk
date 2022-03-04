//package io.tapdata.connector.mongodb;
//
//import com.alibaba.fastjson.JSON;
//import com.mongodb.ReadConcern;
//import com.mongodb.ReadPreference;
//import com.mongodb.WriteConcern;
//import com.mongodb.client.*;
//import io.tapdata.pdk.core.annotations.SourceConnectorClass;
//import io.tapdata.pdk.core.constants.DataSourceConstants;
//import io.tapdata.pdk.core.data.TransformData;
//import io.tapdata.pdk.core.entity.ConnectorReadState;
//import io.tapdata.connector.CoreSourceConnector;
//import io.tapdata.connector.mongodb.connections.MongodbConnectionFactory;
//import org.bson.Document;
//import org.bson.types.ObjectId;
//
//import java.util.Map;
//
//
//@SourceConnectorClass(DataSourceConstants.MongoDB)
//public class MongoDBSourceConnector extends CoreSourceConnector {
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
//    public void verifyConnection() {
//
//    }
//
//    @Override
//    public void doInitializing(ConnectorReadState connectorReadState) {
//        MongoCollection<Document> collection = MongodbConnectionFactory.getInstance().getCollection(sourceNode);
//        if(sourceOptions.isInitial()) {
//            FindIterable<Document> iterable = collection.find();
//            iterable.noCursorTimeout(true);
//            MongoCursor<Document> cursor = iterable.cursor();
//            while(cursor.hasNext()) {
//                Document document = cursor.next();
//
//                sourceNode.getStreamingQueue().offer(new TransformData(sourceNode.getTable(), convertToMap(document)));
//            }
//        }
//    }
//
//    private Map<String, Object> convertToMap(Document document) {
//        Object objectId = document.get("_id");
//        document.put("_id", objectId.toString());
//        return document;
//    }
//
//    @Override
//    public ConnectorReadState getRuntimeConnectorReadState() {
//        return null;
//    }
//
//    @Override
//    public void doCDC(ConnectorReadState connectorReadState) {
//
//    }
//
//}
