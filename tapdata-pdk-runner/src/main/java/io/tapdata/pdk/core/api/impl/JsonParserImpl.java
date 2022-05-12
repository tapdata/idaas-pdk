package io.tapdata.pdk.core.api.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.DefaultJSONParser;
import com.alibaba.fastjson.parser.Feature;
import com.alibaba.fastjson.parser.ParserConfig;
import com.alibaba.fastjson.parser.deserializer.AbstractDateDeserializer;
import com.alibaba.fastjson.parser.deserializer.ObjectDeserializer;
import com.alibaba.fastjson.serializer.SerializerFeature;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.*;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.entity.utils.TypeHolder;
import io.tapdata.entity.annotations.Implementation;
import io.tapdata.pdk.core.utils.TapConstants;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;

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
        return JSON.toJSONString(obj, SerializerFeature.DisableCircularReferenceDetect/*, SerializerFeature.SortField, SerializerFeature.MapSortField*/);
    }

    @Override
    public DataMap fromJson(String json) {
        return JSON.parseObject(json, DataMap.class, Feature.OrderedField, /*Feature.UseNativeJavaObject, */Feature.DisableCircularReferenceDetect);
    }

    @Override
    public <T> T fromJson(String json, Class<T> clazz) {
        return JSON.parseObject(json, clazz, Feature.OrderedField, /*Feature.UseNativeJavaObject, */Feature.DisableCircularReferenceDetect);
    }

    @Override
    public <T> T fromJson(String json, Class<T> clazz, List<AbstractClassDetector> abstractClassDetectors) {
        ParserConfig parserConfig = null;
        if(abstractClassDetectors != null && !abstractClassDetectors.isEmpty()) {
            parserConfig = new ParserConfig() {
                @Override
                public ObjectDeserializer getDeserializer(Type type) {
                    if (type == TapType.class) {
                        return new AbstractResultDeserializer(abstractClassDetectors);
                    }
                    return super.getDeserializer(type);
                }
            };
        }
        return JSON.parseObject(json, clazz, parserConfig, Feature.OrderedField, /*Feature.UseNativeJavaObject, */Feature.DisableCircularReferenceDetect);
    }

    @Override
    public <T> T fromJson(String json, TypeHolder<T> typeHolder) {
        return JSON.parseObject(json, typeHolder.getType(), Feature.OrderedField, /*Feature.UseNativeJavaObject, */Feature.DisableCircularReferenceDetect);
    }

    @Override
    public <T> T fromJson(String json, TypeHolder<T> typeHolder, List<AbstractClassDetector> abstractClassDetectors) {
        ParserConfig parserConfig = null;
        if(abstractClassDetectors != null && !abstractClassDetectors.isEmpty()) {
            parserConfig = new ParserConfig() {
                @Override
                public ObjectDeserializer getDeserializer(Type type) {
                    if (type == TapType.class) {
                        return new AbstractResultDeserializer(abstractClassDetectors);
                    }
                    return super.getDeserializer(type);
                }
            };
        }
        return JSON.parseObject(json, typeHolder.getType(), parserConfig, Feature.OrderedField, /*Feature.UseNativeJavaObject, */Feature.DisableCircularReferenceDetect);
    }

    public static void main(String[] args) {
        TapTable tapTable = new TapTable("aa")
                .add(new TapField("aaa", "bbb").tapType(new TapString().bytes(123L)))
                .add(new TapField("aacc", "aaa").tapType(new TapNumber().bit(2334)))
                .add(new TapField("aaa1", "adsf").tapType(new TapDateTime().fraction(234)))
                ;


//        String str = JSON.toJSONString(tapTable, SerializerFeature.WriteClassName);
//        TapTable t = (TapTable) JSON.parse(str, Feature.SupportAutoType);

        String str = JSON.toJSONString(tapTable);

        TapTable t1 = InstanceFactory.instance(JsonParser.class).fromJson(str, TapTable.class,
                TapConstants.abstractClassDetectors);

    }
}
