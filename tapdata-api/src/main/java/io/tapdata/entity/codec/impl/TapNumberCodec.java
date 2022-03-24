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
        TapType tapType = tapValue.getTapType();
        TapNumber tapNumber = null;
        if(tapType instanceof TapNumber) {
            tapNumber = (TapNumber) tapType;
        }
        if(tapNumber != null) {
            Integer scale = tapNumber.getScale();
            //TODO need more code
            if(scale == null || scale == 0)
                return tapValue.getValue().longValue();
            else
                return tapValue.getValue();
        }
        return tapValue.getValue();
    }

    @Override
    public TapNumberValue toTapValue(Object value) {

        TapNumberValue numberValue = null;
        if(value instanceof Number) {
            numberValue = new TapNumberValue(((Number) value).doubleValue());
        }

        return numberValue;
    }
}
