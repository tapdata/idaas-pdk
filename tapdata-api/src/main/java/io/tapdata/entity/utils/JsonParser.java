package io.tapdata.entity.utils;

public interface JsonParser {
    String toJson(Object obj);
    DefaultMap fromJson(String json);
    <T> T fromJson(String json, Class<T> clazz);
}
