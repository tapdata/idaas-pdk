package io.tapdata.entity.mapping.type;

import io.tapdata.entity.type.TapMap;
import io.tapdata.entity.type.TapTime;
import io.tapdata.entity.type.TapType;

import java.util.Map;

/**
 * "time": {"range": ["-838:59:59","838:59:59"], "to": "typeInterval:typeNumber"},
 */
public class TapTimeMapping extends TapDateBase {

    @Override
    protected String pattern() {
        return "HH:mm:ss";
    }

    @Override
    public TapType toTapType(String originType, Map<String, String> params) {
        return new TapTime();
    }

    @Override
    public String fromTapType(String typeExpression, TapType tapType) {
        if (tapType instanceof TapTime) {
            return removeBracketVariables(typeExpression, 0);
        }
        return null;
    }
}
