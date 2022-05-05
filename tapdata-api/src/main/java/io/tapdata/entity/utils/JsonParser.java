package io.tapdata.entity.utils;

public interface JsonParser {
    String toJson(Object obj);
    DataMap fromJson(String json);
    <T> T fromJson(String json, Class<T> clazz);
    <T> T fromJson(String json, TypeHolder<T> typeHolder);

    String toJsonWithClass(Object obj);
    Object fromJsonWithClass(String json);
    Object fromJsonWithClass(String json, ClassLoader classLoader);
}
