package io.tapdata.entity.mapping.type;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapTime;
import io.tapdata.entity.schema.type.TapType;

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

    @Override
    public long matchingScore(TapField field) {
        if (field.getTapType() instanceof TapTime) {
            return 0L;
        }
        return -1L;
    }
}
