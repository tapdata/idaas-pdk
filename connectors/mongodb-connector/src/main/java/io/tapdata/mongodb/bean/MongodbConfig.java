package io.tapdata.mongodb.bean;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

public class MongodbConfig implements Serializable {
    private String database;
    private String collection;
    private String uri;

    public static MongodbConfig load(String jsonFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new JsonFactory());
        return mapper.readValue(new File(jsonFile), MongodbConfig.class);
    }

    public static MongodbConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue((new ObjectMapper()).writeValueAsString(map), MongodbConfig.class);
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

}
