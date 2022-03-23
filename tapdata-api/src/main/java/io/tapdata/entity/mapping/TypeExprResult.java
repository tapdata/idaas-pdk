package io.tapdata.entity.mapping;

import java.util.Map;

public class TypeExprResult<T> {
    private T value;
    private Map<String, String> params;

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public void setParams(Map<String, String> params) {
        this.params = params;
    }
}
