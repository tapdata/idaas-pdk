package io.tapdata.entity.utils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InstanceFactory {
    private static Map<Class<?>, Object> instanceMap = new ConcurrentHashMap<>();
    private InstanceFactory() {}
    public static <T> T instance(Class<T> instanceClass) {
        return (T) instanceMap.computeIfAbsent(instanceClass, aClass -> ClassFactory.create(instanceClass));
    }
}
