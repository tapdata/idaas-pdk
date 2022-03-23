package io.tapdata.entity.mapping.type;

import io.tapdata.entity.type.TapMap;
import io.tapdata.entity.type.TapType;
import io.tapdata.entity.utils.DefaultMap;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class TapMapping {
    private String to;
    private static final Map<String, Class<?>> classCacheMap = new ConcurrentHashMap<>();

    public TapMapping() {}

    public abstract void from(Map<String, Object> info);

    public static TapMapping build(DefaultMap info) {
        return TapMapping.build(((Map<String, Object>)info));
    }
    public static TapMapping build(Map<String, Object> info) {
        String to = (String) info.get("to");
        if(to == null)
            return null;

        String typeMappingClass = TapMapping.class.getPackage().getName() + "." + to + "Mapping";
        Class<?> mappingClass = classCacheMap.get(typeMappingClass);
        if(mappingClass == null) {
            synchronized (classCacheMap) {
                mappingClass = classCacheMap.get(typeMappingClass);
                if(mappingClass == null) {
                    try {
                        mappingClass = Class.forName(typeMappingClass);
                        if(!TapMapping.class.isAssignableFrom(mappingClass)) {
                            return null;
                        }
                        classCacheMap.put(typeMappingClass, mappingClass);
                    } catch (Throwable e) {
                        e.printStackTrace();
                        return null;
                    }
                }
            }
        }

        try {
            TapMapping tapMapping = (TapMapping) mappingClass.getConstructor().newInstance();
            tapMapping.to = to;
            tapMapping.from(info);
            return tapMapping;
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return null;
    }

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }
}
