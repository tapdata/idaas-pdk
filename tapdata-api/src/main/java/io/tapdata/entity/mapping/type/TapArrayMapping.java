package io.tapdata.entity.mapping.type;

import io.tapdata.entity.schema.type.TapArray;
import io.tapdata.entity.schema.type.TapType;

import java.util.Map;

/**
 */
public class TapArrayMapping extends TapSizeBase {


    @Override
    public TapType toTapType(String originType, Map<String, String> params) {
        return new TapArray();
    }

    @Override
    public String fromTapType(String typeExpression, TapType tapType) {
        if (tapType instanceof TapArray) {
            return removeBracketVariables(typeExpression, 0);
        }
        return null;
    }
}
