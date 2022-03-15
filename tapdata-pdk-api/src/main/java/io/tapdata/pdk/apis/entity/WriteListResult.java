package io.tapdata.pdk.apis.entity;

import java.util.Map;

public class WriteListResult<T> {
    private int insertedCount;
    public WriteListResult<T> insertedCount(int insertedCount) {
        this.insertedCount = insertedCount;
        return this;
    }
    private int removedCount;
    public WriteListResult<T> removedCount(int removedCount) {
        this.removedCount = removedCount;
        return this;
    }
    private int modifiedCount;
    public WriteListResult<T> modifiedCount(int modifiedCount) {
        this.modifiedCount = modifiedCount;
        return this;
    }

    private Map<T, Throwable> errorMap;

    public WriteListResult() {}
    public WriteListResult(int insertedCount, int modifiedCount, int removedCount) {
        this(insertedCount, modifiedCount, removedCount, null);
    }
    public WriteListResult(int insertedCount, int modifiedCount, int removedCount, Map<T, Throwable> errorMap) {
        this.insertedCount = insertedCount;
        this.modifiedCount = modifiedCount;
        this.removedCount = removedCount;
        this.errorMap = errorMap;
    }

    public int getInsertedCount() {
        return insertedCount;
    }

    public void setInsertedCount(int insertedCount) {
        this.insertedCount = insertedCount;
    }

    public int getRemovedCount() {
        return removedCount;
    }

    public void setRemovedCount(int removedCount) {
        this.removedCount = removedCount;
    }

    public int getModifiedCount() {
        return modifiedCount;
    }

    public void setModifiedCount(int modifiedCount) {
        this.modifiedCount = modifiedCount;
    }

    public Map<T, Throwable> getErrorMap() {
        return errorMap;
    }

    public void setErrorMap(Map<T, Throwable> errorMap) {
        this.errorMap = errorMap;
    }
}
