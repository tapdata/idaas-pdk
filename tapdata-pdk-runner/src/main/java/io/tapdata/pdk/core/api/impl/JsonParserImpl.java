package io.tapdata.pdk.core.api.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.Feature;
import com.alibaba.fastjson.serializer.SerializerFeature;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.entity.utils.TypeHolder;
import io.tapdata.entity.annotations.Implementation;

@Implementation(JsonParser.class)
public class JsonParserImpl implements JsonParser {
    private static final String PREFIX = "TapClass#{";
    private static final String SUFFIX = "}#";

    @Override
    public String toJsonWithClass(Object obj) {
        return PREFIX + obj.getClass().getName() + SUFFIX + JSON.toJSONString(obj);
    }
    @Override
    public Object fromJsonWithClass(String json) {
        return fromJsonWithClass(json, null);
    }
    @Override
    public Object fromJsonWithClass(String json, ClassLoader classLoader) {
        if(json.startsWith(PREFIX)) {
            int pos = json.indexOf(SUFFIX);
            if(pos > 0) {
                String classStr = json.substring(PREFIX.length(), pos);
                if(classLoader != null) {
                    try {
                        Class<?> clazz = classLoader.loadClass(classStr);
                        return JSON.parseObject(json.substring(pos + SUFFIX.length()), clazz);
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        return null;
    }

    @Override
    public String toJson(Object obj) {
        return JSON.toJSONString(obj, SerializerFeature.DisableCircularReferenceDetect, SerializerFeature.SortField, SerializerFeature.MapSortField);
    }

    @Override
    public DataMap fromJson(String json) {
        return JSON.parseObject(json, DataMap.class, Feature.OrderedField, Feature.UseNativeJavaObject, Feature.DisableCircularReferenceDetect);
    }

    @Override
    public <T> T fromJson(String json, Class<T> clazz) {
        return JSON.parseObject(json, clazz, Feature.OrderedField, Feature.UseNativeJavaObject, Feature.DisableCircularReferenceDetect);
    }

    @Override
    public <T> T fromJson(String json, TypeHolder<T> typeHolder) {
        return JSON.parseObject(json, typeHolder.getType(), Feature.OrderedField, Feature.UseNativeJavaObject, Feature.DisableCircularReferenceDetect);
    }
}
