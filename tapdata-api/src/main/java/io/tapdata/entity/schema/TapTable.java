package io.tapdata.entity.schema;

import java.util.*;
import java.util.concurrent.atomic.LongAdder;

public class TapTable extends TapItem<TapField> {
    public TapTable() {}
    public TapTable(String nameAndId) {
        this(nameAndId, nameAndId);
    }
    public TapTable(String id, String name) {
        this.name = name;
        this.id = id;
    }

    /**
     * Please don't add or remove on this field. If you need to add or delete, please set new one.
     * Protect ConcurrentModification.
     */
    private LinkedHashMap<String, TapField> nameFieldMap;

    /**
     * For the database which don't need create table before insert records.
     *
     * Given the default primary keys, if user don't give the specific primary key, will use defaultPrimaryKeys as primary keys.
     */
    private List<String> defaultPrimaryKeys;
    public TapTable defaultPrimaryKeys(List<String> defaultPrimaryKeys) {
        this.defaultPrimaryKeys = defaultPrimaryKeys;
        return this;
    }

    private List<TapIndex> indexList;

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

    public TapTable add(TapIndex index) {
        indexCheck(index);
        if(indexList == null) {
            indexList = new ArrayList<>();
        }
        indexList.add(index);
        return this;
    }

    public TapTable add(TapField field) {
        fieldCheck(field);
        if(nameFieldMap == null) {
            nameFieldMap = new LinkedHashMap<>();
        }
        nameFieldMap.put(field.getName(), field);
        if(field.getPos() == null) {
            field.pos(nameFieldMap.size());
        }
        return this;
    }

    private void indexCheck(TapIndex index) {
        if(index == null)
            throw new IllegalArgumentException("index is null when add into table " + name);
        if(index.getFields() == null || index.getFields().isEmpty())
            throw new IllegalArgumentException("index fields is null or empty when add into table " + name);
    }

    private void fieldCheck(TapField field) {
        if(field == null)
            throw new IllegalArgumentException("field is null when add into table " + name);
        if(field.getName() == null)
            throw new IllegalArgumentException("field name is null when add into table " + name);
    }

    public Collection<String> primaryKeys() {
        LinkedHashMap<String, TapField> nameFieldMapCopyRef = this.nameFieldMap;
        if(nameFieldMapCopyRef == null)
            return Collections.emptyList();

        Map<Integer, String> posPrimaryKeyName = new TreeMap<>();
        for (String key : nameFieldMapCopyRef.keySet()) {
            TapField field = nameFieldMapCopyRef.get(key);
            if (field != null && field.getPrimaryKey() != null && field.getPrimaryKey()) {
                posPrimaryKeyName.put(field.getPrimaryKeyPos(), field.getName());
            }
        }
        return posPrimaryKeyName.values();
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

    public List<TapIndex> getIndexList() {
        return indexList;
    }

    public void setIndexList(List<TapIndex> indexList) {
        this.indexList = indexList;
    }

    public List<String> getDefaultPrimaryKeys() {
        return defaultPrimaryKeys;
    }

    public void setDefaultPrimaryKeys(List<String> defaultPrimaryKeys) {
        this.defaultPrimaryKeys = defaultPrimaryKeys;
    }
}
