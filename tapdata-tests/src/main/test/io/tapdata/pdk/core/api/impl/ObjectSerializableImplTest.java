package io.tapdata.pdk.core.api.impl;

import com.alibaba.fastjson.JSONObject;
import io.tapdata.entity.codecs.TDDUser;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.ObjectSerializable;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ObjectSerializableImplTest {
    @Test
    public void test() {
        ObjectSerializable objectSerializable = InstanceFactory.instance(ObjectSerializable.class);
        byte[] doubleBytes = objectSerializable.fromObject(123.1d);
        Double doubleValue = (Double) objectSerializable.toObject(doubleBytes);
        assertEquals(123.1d, doubleValue);

        TDDUser user = new TDDUser();
        user.setUserId("abc");
        user.setName("aaa");
        TDDUser newUser = (TDDUser) objectSerializable.toObject(objectSerializable.fromObject(user));
        assertEquals(user.getUserId(), newUser.getUserId());
        assertEquals(user.getName(), newUser.getName());

        List<TDDUser> users = new ArrayList<>();
        users.add(user);
        List<TDDUser> newUsers = (List<TDDUser>) objectSerializable.toObject(objectSerializable.fromObject(users));
        assertEquals(users.size(), newUsers.size());
        assertEquals(users.get(0).getName(), newUsers.get(0).getName());

        Map<String, TDDUser> userMap = new HashMap<>();
        userMap.put("aaa", user);
        userMap.put("bbb", user);
        Map<String, TDDUser> newUserMap = (Map<String, TDDUser>) objectSerializable.toObject(objectSerializable.fromObject(userMap));
        assertEquals(userMap.size(), newUserMap.size());
        assertEquals(userMap.get("aaa").getName(), newUserMap.get("aaa").getName());
        assertEquals(userMap.get("bbb").getName(), newUserMap.get("bbb").getName());

        Document document = new Document();
        BsonTimestamp bsonTimestamp = new BsonTimestamp(new Date().getTime());
        ObjectId objectId = ObjectId.get();
        document.put("timestamp", bsonTimestamp);
        document.put("oid", objectId);
        Document embeddedDocument = new Document();
        embeddedDocument.put("aaa", 123);
        document.put("doc", embeddedDocument);
        Document newDocument = (Document) objectSerializable.toObject(objectSerializable.fromObject(document));
        assertEquals(((BsonTimestamp)newDocument.get("timestamp")).getValue(), bsonTimestamp.getValue());
        assertEquals(newDocument.get("oid").toString(), objectId.toString());
        assertEquals(((Document)newDocument.get("doc")).get("aaa"), 123);

        List<Document> documents = new ArrayList<>();
        documents.add(document);
        List<Document> newDocuments = (List<Document>) objectSerializable.toObject(objectSerializable.fromObject(documents));
        assertEquals(documents.size(), newDocuments.size());
        Document doc1 = newDocuments.get(0);
        assertEquals(((BsonTimestamp)doc1.get("timestamp")).getValue(), bsonTimestamp.getValue());
        assertEquals(doc1.get("oid").toString(), objectId.toString());
        assertEquals(((Document)doc1.get("doc")).get("aaa"), 123);

        Map<TDDUser, Document> documentMap = new HashMap<>();
        documentMap.put(user, document);
        Map<TDDUser, Document> newDocumentMap = (Map<TDDUser, Document>) objectSerializable.toObject(objectSerializable.fromObject(documentMap));
        assertEquals(documentMap.size(), newDocumentMap.size());
        TDDUser userKey = documentMap.keySet().stream().findFirst().get();
        assertEquals(user.getName(), userKey.getName());
        Document doc2 = documentMap.get(userKey);
        assertEquals(((BsonTimestamp)doc2.get("timestamp")).getValue(), bsonTimestamp.getValue());
        assertEquals(doc2.get("oid").toString(), objectId.toString());
        assertEquals(((Document)doc2.get("doc")).get("aaa"), 123);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("aaa", "BBB");
        JSONObject newJsonObject = (JSONObject) objectSerializable.toObject(objectSerializable.fromObject(jsonObject));
        assertEquals(jsonObject.get("aaa"), newJsonObject.get("aaa"));
    }

}
