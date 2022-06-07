package io.tapdata.pdk.apis.entity;

import java.util.List;

public class Projection {
    private List<String> includeFields;
    public Projection includeFields(List<String> includeFields) {
        this.includeFields = includeFields;
        return this;
    }
    private List<String> excludeFields;
    public Projection excludeFields(List<String> excludeFields) {
        this.excludeFields = excludeFields;
        return this;
    }

    public List<String> getIncludeFields() {
        return includeFields;
    }

    public void setIncludeFields(List<String> includeFields) {
        this.includeFields = includeFields;
    }

    public List<String> getExcludeFields() {
        return excludeFields;
    }

    public void setExcludeFields(List<String> excludeFields) {
        this.excludeFields = excludeFields;
    }
}
