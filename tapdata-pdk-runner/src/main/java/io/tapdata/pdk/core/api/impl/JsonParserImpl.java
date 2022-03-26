package io.tapdata.pdk.core.api.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.Feature;
import com.alibaba.fastjson.serializer.SerializerFeature;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.entity.utils.TypeHolder;
import io.tapdata.pdk.core.annotations.Implementation;

@Implementation(JsonParser.class)
public class JsonParserImpl implements JsonParser {
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
