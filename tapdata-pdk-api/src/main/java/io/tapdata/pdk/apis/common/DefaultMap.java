package io.tapdata.pdk.apis.common;


import java.util.LinkedHashMap;

/**
 *
 */
public class DefaultMap extends LinkedHashMap<String, Object> {
    public <T> DefaultMap putValue(String key, T value) {
        super.put(key, value);
        return this;
    }

    public <T> T getValue(String key, T defaultValue) {
        T value = (T) super.get(key);
        if(value == null) {
            return defaultValue;
        }
        return value;
    }
}
