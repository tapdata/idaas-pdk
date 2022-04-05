package io.tapdata.entity.codec.impl;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.TapDefaultCodecs;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.TapNumberValue;

@Implementation(value = FromTapValueCodec.class, type = TapDefaultCodecs.TAP_NUMBER_VALUE, buildNumber = 0)
public class FromTapNumberCodec implements FromTapValueCodec<TapNumberValue> {
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

}
