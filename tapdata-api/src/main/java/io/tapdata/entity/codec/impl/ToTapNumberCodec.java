package io.tapdata.entity.codec.impl;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.codec.TapDefaultCodecs;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.schema.value.TapNumberValue;

@Implementation(value = ToTapValueCodec.class, type = TapDefaultCodecs.TAP_NUMBER_VALUE, buildNumber = 0)
public class ToTapNumberCodec implements ToTapValueCodec<TapNumberValue> {
    @Override
    public TapNumberValue toTapValue(Object value) {

        TapNumberValue numberValue = null;
        if(value instanceof Number) {
            numberValue = new TapNumberValue(Double.valueOf(String.valueOf(value)));
        }

        return numberValue;
    }
}
