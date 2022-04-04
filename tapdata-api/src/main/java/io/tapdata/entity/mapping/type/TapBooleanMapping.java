package io.tapdata.entity.mapping.type;

import io.tapdata.entity.type.TapBoolean;
import io.tapdata.entity.type.TapDate;
import io.tapdata.entity.type.TapType;

import java.util.Map;

/**
 */
public class TapBooleanMapping extends TapMapping {


    @Override
    public void from(Map<String, Object> info) {

    }

    @Override
    public TapType toTapType(String originType, Map<String, String> params) {
        return new TapBoolean();
    }

    @Override
    public String fromTapType(String typeExpression, TapType tapType) {
        if (tapType instanceof TapBoolean) {
            return removeBracketVariables(typeExpression, 0);
        }
        return null;
    }
}
