
package io.tapdata.entity.mapping;

import io.tapdata.entity.utils.DefaultMap;

import java.util.Map;

public class DefaultExpressionMatchingMap extends ExpressionMatchingMap<DefaultMap> {
    public DefaultExpressionMatchingMap(Map<String, DefaultMap> map) {
        super(map);
    }
}
