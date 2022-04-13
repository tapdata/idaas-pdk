package io.tapdata.mongodb;

import org.bson.types.ObjectId;

import java.util.Collection;

public class MongoOffset {
    private String sortKey;
    private Object value;
    private Boolean isObjectId;

    public MongoOffset() {}
    public MongoOffset(String sortKey, Object value) {
        setValue(value);
        this.sortKey = sortKey;
    }

    public Object value() {
        if(isObjectId != null && isObjectId && (value instanceof String)) {
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
            this.isObjectId = true;
        } else {
            this.value = value;
            this.isObjectId = null;
        }
    }

    public Boolean getObjectId() {
        return isObjectId;
    }

    public void setObjectId(Boolean objectId) {
        isObjectId = objectId;
    }
}
