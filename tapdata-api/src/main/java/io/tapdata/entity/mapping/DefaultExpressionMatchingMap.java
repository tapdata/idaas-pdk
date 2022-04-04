
package io.tapdata.entity.mapping;

import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.entity.utils.TypeHolder;

import java.util.Map;

public class DefaultExpressionMatchingMap extends ExpressionMatchingMap<DataMap> {
    public DefaultExpressionMatchingMap(Map<String, DataMap> map) {
        super(map);
    }

    public static DefaultExpressionMatchingMap map(String json) {
        return new DefaultExpressionMatchingMap(InstanceFactory.instance(JsonParser.class).fromJson(json, new TypeHolder<Map<String, DataMap>>(){}));
    }

    public static DefaultExpressionMatchingMap map(Map<String, DataMap> map) {
        return new DefaultExpressionMatchingMap(map);
    }
}
