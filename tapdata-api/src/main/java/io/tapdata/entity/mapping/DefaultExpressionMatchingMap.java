
package io.tapdata.entity.mapping;

import io.tapdata.entity.utils.DataMap;

import java.util.Map;

public class DefaultExpressionMatchingMap extends ExpressionMatchingMap<DataMap> {
    public DefaultExpressionMatchingMap(Map<String, DataMap> map) {
        super(map);
    }
}
