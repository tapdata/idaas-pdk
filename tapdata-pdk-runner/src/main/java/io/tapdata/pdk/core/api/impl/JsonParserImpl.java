package io.tapdata.pdk.core.api.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.Feature;
import com.alibaba.fastjson.serializer.SerializerFeature;
import io.tapdata.entity.utils.DefaultMap;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.pdk.core.annotations.Implementation;

import java.util.Map;

@Implementation(JsonParser.class)
public class JsonParserImpl implements JsonParser {
    @Override
    public String toJson(Object obj) {
        return JSON.toJSONString(obj, SerializerFeature.DisableCircularReferenceDetect, SerializerFeature.SortField, SerializerFeature.MapSortField);
    }

    @Override
    public DefaultMap fromJson(String json) {
        return JSON.parseObject(json, DefaultMap.class, Feature.OrderedField, Feature.UseNativeJavaObject, Feature.DisableCircularReferenceDetect);
    }

    @Override
    public <T> T fromJson(String json, Class<T> clazz) {
        return JSON.parseObject(json, clazz, Feature.OrderedField, Feature.UseNativeJavaObject, Feature.DisableCircularReferenceDetect);
    }
}
