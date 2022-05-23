package io.tapdata.mongodb;

import org.bson.types.ObjectId;

public class MongoBatchOffset {
    private String sortKey;
    private Object value;
    private Boolean objectId;

    public MongoBatchOffset() {}
    public MongoBatchOffset(String sortKey, Object value) {
        setValue(value);
        this.sortKey = sortKey;
    }

    public Object value() {
        if(objectId != null && objectId && (value instanceof String)) {
            return new ObjectId((String) value);
        }
        return value;
    }

    public String getSortKey() {
        return sortKey;
    }

    public void setSortKey(String sortKey) {
        this.sortKey = sortKey;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        if(value instanceof ObjectId) {
            this.value = ((ObjectId)value).toString();
            this.objectId = true;
        } else {
            this.value = value;
            this.objectId = null;
        }
    }

    public Boolean getObjectId() {
        return objectId;
    }

    public void setObjectId(Boolean objectId) {
        this.objectId = objectId;
    }
}
