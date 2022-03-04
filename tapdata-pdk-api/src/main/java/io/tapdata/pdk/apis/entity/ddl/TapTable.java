package io.tapdata.pdk.apis.entity.ddl;

import io.tapdata.pdk.apis.common.DefaultMap;

import java.util.List;
import java.util.Map;

public class TapTable {
    private String name;
    private String id;
    private Map<String, TapField> nameFieldMap;
    private List<String> primaryKeys;

    private DefaultMap info;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public DefaultMap getInfo() {
        return info;
    }

    public void setInfo(DefaultMap info) {
        this.info = info;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, TapField> getNameFieldMap() {
        return nameFieldMap;
    }

    public void setNameFieldMap(Map<String, TapField> nameFieldMap) {
        this.nameFieldMap = nameFieldMap;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public void setPrimaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }
}
