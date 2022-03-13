package io.tapdata.entity.codec.impl;

import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.type.TapBoolean;
import io.tapdata.entity.type.TapType;
import io.tapdata.entity.value.TapBooleanValue;

public class TapBooleanCodec implements ToTapValueCodec<TapBooleanValue>, FromTapValueCodec<TapBooleanValue> {
    @Override
    public Object fromTapValue(TapBooleanValue tapValue) {
        if(tapValue == null)
            return null;
        TapBoolean tapNumber = tapValue.getTapType();
        //TODO need more code
        return tapValue.getValue();
    }

    @Override
    public TapBooleanValue toTapValue(Object value, String originType, TapType typeFromSchema) {
        if(value == null)
            return null;
        TapType tapType;
        if(typeFromSchema instanceof TapBoolean) {
            tapType = typeFromSchema;
        } else {
            //type is not found in schema
            //type is not expected as schema wanted. type and value will be reserved
            tapType = new TapBoolean();
        }
        TapBooleanValue numberValue = null;
        if(value instanceof Number) {
            numberValue = new TapBooleanValue(((Number) value).intValue() != 0);
        } else if(value instanceof Boolean) {
            numberValue = new TapBooleanValue((Boolean) value);
        }
        if(numberValue != null) {
            numberValue.setTapType((TapBoolean) tapType);
            numberValue.setOriginValue(value);
            numberValue.setOriginType(originType);
        }

        return numberValue;
    }
}
