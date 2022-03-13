package io.tapdata.entity.codec.impl;

import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.type.TapNumber;
import io.tapdata.entity.type.TapType;
import io.tapdata.entity.value.TapNumberValue;

public class TapNumberCodec implements ToTapValueCodec<TapNumberValue>, FromTapValueCodec<TapNumberValue> {
    @Override
    public Object fromTapValue(TapNumberValue tapValue) {
        if(tapValue == null)
            return null;
        TapNumber tapNumber = tapValue.getTapType();
        if(tapNumber != null) {
            Long scale = tapNumber.getScale();
            //TODO need more code
            if(scale == null || scale == 0)
                return tapValue.getValue().longValue();
            else
                return tapValue.getValue();
        }
        return tapValue.getValue();
    }

    @Override
    public TapNumberValue toTapValue(Object value, String originType, TapType typeFromSchema) {
        if(value == null)
            return null;
        TapType tapType;
        if(typeFromSchema instanceof TapNumber) {
            tapType = typeFromSchema;
        } else {
            //type is not found in schema
            //type is not expected as schema wanted. type and value will be reserved
            tapType = new TapNumber();
        }
        TapNumberValue numberValue = null;
        if(value instanceof Number) {
            numberValue = new TapNumberValue(((Number) value).doubleValue());
            numberValue.setTapType((TapNumber) tapType);
            numberValue.setOriginValue(value);
            numberValue.setOriginType(originType);
        }

        return numberValue;
    }
}
