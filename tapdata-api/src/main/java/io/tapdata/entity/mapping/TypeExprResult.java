package io.tapdata.entity.mapping;

import java.util.Map;

public class TypeExprResult {
    private Map<String, Object> value;
    private Map<String, String> params;

    public Map<String, Object> getValue() {
        return value;
    }

    public void setValue(Map<String, Object> value) {
        this.value = value;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public void setParams(Map<String, String> params) {
        this.params = params;
    }
}
