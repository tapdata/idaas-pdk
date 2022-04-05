package io.tapdata.entity.mapping.type;

import io.tapdata.entity.schema.type.TapMap;
import io.tapdata.entity.schema.type.TapType;

import java.util.Map;

/**
 */
public class TapMapMapping extends TapSizeBase {


    @Override
    public TapType toTapType(String originType, Map<String, String> params) {
        return new TapMap();
    }

    @Override
    public String fromTapType(String typeExpression, TapType tapType) {
        if (tapType instanceof TapMap) {
            return removeBracketVariables(typeExpression, 0);
        }
        return null;
    }
}
