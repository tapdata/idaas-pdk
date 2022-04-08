package io.tapdata.entity.mapping.type;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.type.TapType;

import java.util.Map;

/**
 * "datetime": {"range": ["1000-01-01 00:00:00", "9999-12-31 23:59:59"], "to": "typeDateTime"},
 */
public class TapDateTimeMapping extends TapDateBase {

    @Override
    protected String pattern() {
        return "yyyy-MM-dd HH:mm:ss";
    }


    @Override
    public TapType toTapType(String originType, Map<String, String> params) {
        return new TapDateTime();
    }

    @Override
    public String fromTapType(String typeExpression, TapType tapType) {
        if (tapType instanceof TapDateTime) {
            return removeBracketVariables(typeExpression, 0);
        }
        return null;
    }

    @Override
    public long matchingScore(TapField field) {
        if (field.getTapType() instanceof TapDateTime) {
            return 0L;
        }
        return -1L;
    }
}
