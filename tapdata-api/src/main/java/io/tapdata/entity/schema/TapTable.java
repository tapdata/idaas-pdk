package io.tapdata.entity.schema;

import java.util.Collection;
import java.util.LinkedHashMap;

public class TapTable extends TapItem<TapField> {
    public TapTable() {}
    public TapTable(String nameAndId) {
        this(nameAndId, nameAndId);
    }
    public TapTable(String id, String name) {
        this.name = name;
        this.id = id;
    }

    private LinkedHashMap<String, TapField> nameFieldMap;

    private String id;

    private String name;
    /**
     * 存储引擎， innoDB
     */
    private String storageEngine;
    /**
     * 字符编码
     */
    private String charset;

    public String toString() {
        return "TapTable id " + id +
                " name " + name +
                " storageEngine " + storageEngine +
                " charset " + charset +
                " number of fields " + (nameFieldMap != null ? nameFieldMap.size() : 0);
    }

    public TapTable add(TapField field) {
        fieldCheck(field);
        if(nameFieldMap == null) {
            nameFieldMap = new LinkedHashMap<>();
        }
        nameFieldMap.put(field.getName(), field);
        return this;
    }

    private void fieldCheck(TapField field) {
        if(field == null)
            throw new IllegalArgumentException("field is null when add into table " + name);
        if(field.getName() == null)
            throw new NullPointerException("field name is null when add into table " + name);
    }

    @Override
    public Collection<TapField> childItems() {
        if(nameFieldMap != null)
            return nameFieldMap.values();
        return null;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void putField(String name, TapField field) {
        field.setName(name);
        add(field);
    }

    public LinkedHashMap<String, TapField> getNameFieldMap() {
        return nameFieldMap;
    }

    public void setNameFieldMap(LinkedHashMap<String, TapField> nameFieldMap) {
        this.nameFieldMap = nameFieldMap;
    }

    public String getStorageEngine() {
        return storageEngine;
    }

    public void setStorageEngine(String storageEngine) {
        this.storageEngine = storageEngine;
    }

    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
