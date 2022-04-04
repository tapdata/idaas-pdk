package io.tapdata.entity.codec.filter.impl;

import java.util.Map;

public class TapEntry implements Map.Entry<String, Object> {
    private Map.Entry<String, Object> entry;
    private String prefix;
    public TapEntry(Map.Entry<String, Object> entry, String prefix) {
        this.entry = entry;
        this.prefix = prefix;
    }
    @Override
    public String getKey() {
        return this.prefix + entry.getKey();
    }

    @Override
    public Object getValue() {
        return entry.getValue();
    }

    @Override
    public Object setValue(Object value) {
        return entry.setValue(value);
    }
}
