package io.tapdata.entity.mapping.type;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapRaw;
import io.tapdata.entity.schema.type.TapType;

import java.util.Map;

/**
 */
public class TapRawMapping extends TapMapping {


    @Override
    public void from(Map<String, Object> info) {

    }

    @Override
    public TapType toTapType(String originType, Map<String, String> params) {
        return new TapRaw();
    }

    @Override
    public String fromTapType(String typeExpression, TapType tapType) {
        if (tapType instanceof TapRaw) {
            return removeBracketVariables(typeExpression, 0);
        }
        return null;
    }

    @Override
    public long matchingScore(TapField field) {
        if (field.getTapType() instanceof TapRaw) {
            return 0L;
        }
        return -1L;
    }
}
