package io.tapdata.pdk.apis.utils;

import io.tapdata.pdk.apis.common.DefaultMap;

public interface TapUtils {
    void interval(Runnable runnable, int seconds);

    String toJson(Object obj);
    DefaultMap fromJson(String json);
    <T> T fromJson(String json, Class<T> clazz);
}
