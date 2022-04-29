package io.tapdata.entity.graph;

import java.io.Serializable;
import java.util.HashMap;

/**
 * element of graph
 *
 * @author Jarad
 * @date 2022/4/28
 */
public class GraphResource implements Serializable {

    private String id;

    private String name;

    private HashMap<String, Object> properties;

    public GraphResource() {
        properties = new HashMap<>();
    }

    public GraphResource(String idAndName) {
        this(idAndName, idAndName);
    }

    public GraphResource(String id, String name) {
        this();
        this.id = id;
        this.name = name;
    }

    public static GraphResource create(String idAndName) {
        return new GraphResource(idAndName);
    }

    public static GraphResource create(String id, String name) {
        return new GraphResource(id, name);
    }

    public String getId() {
        return id;
    }

    public <T> T id(String id) {
        this.id = id;
        return (T) this;
    }

    public String getName() {
        return name;
    }

    public <T> T name(String name) {
        this.name = name;
        return (T) this;
    }

    public HashMap<String, Object> getProperties() {
        return properties;
    }

    public <T> T properties(HashMap<String, Object> properties) {
        this.properties = properties;
        return (T) this;
    }

    public <T> T addProperty(String key, Object value) {
        this.properties.putIfAbsent(key, value);
        return (T) this;
    }

    public <T> T addProperties(HashMap<String, Object> addProperties) {
        this.properties.putAll(addProperties);
        return (T) this;
    }
}
