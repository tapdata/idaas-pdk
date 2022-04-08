package io.tapdata.entity.mapping.type;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapDate;
import io.tapdata.entity.schema.type.TapType;

import java.util.Map;

/**
 * "date": {"range": ["1000-01-01", "9999-12-31"], "gmt" : 0, "to": "typeDate"},
 */
public class TapDateMapping extends TapDateBase {

    @Override
    protected String pattern() {
        return "yyyy-MM-dd";
    }

    @Override
    public TapType toTapType(String originType, Map<String, String> params) {
        return new TapDate();
    }

    @Override
    public String fromTapType(String typeExpression, TapType tapType) {
        if (tapType instanceof TapDate) {
            return removeBracketVariables(typeExpression, 0);
        }
        return null;
    }

    @Override
    public long matchingScore(TapField field) {
        if (field.getTapType() instanceof TapDate) {
            return 0L;
        }
        return -1L;
    }
}
