package io.tapdata.entity.mapping.type;

import io.tapdata.entity.result.TapResult;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapBoolean;
import io.tapdata.entity.schema.type.TapType;

import java.util.Map;

/**
 */
public class TapBooleanMapping extends TapMapping {


    @Override
    public void from(Map<String, Object> info) {

    }

    @Override
    public TapType toTapType(String dataType, Map<String, String> params) {
        return new TapBoolean();
    }

    @Override
    public TapResult<String> fromTapType(String typeExpression, TapType tapType) {
        if (tapType instanceof TapBoolean) {
            return TapResult.successfully(removeBracketVariables(typeExpression, 0));
        }
        return null;
    }

    @Override
    public long matchingScore(TapField field) {
        if (field.getTapType() instanceof TapBoolean) {
            return 0L;
        }
        return Long.MIN_VALUE;
    }
}
