package io.tapdata.entity.codec.impl;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.codec.TapDefaultCodecs;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.schema.value.TapBooleanValue;

@Implementation(value = ToTapValueCodec.class, type = TapDefaultCodecs.TAP_BOOLEAN_VALUE, buildNumber = 0)
public class ToTapBooleanCodec implements ToTapValueCodec<TapBooleanValue> {
    @Override
    public TapBooleanValue toTapValue(Object value) {
        TapBooleanValue numberValue = null;
        if(value instanceof Number) {
            numberValue = new TapBooleanValue(((Number) value).intValue() != 0);
        } else if(value instanceof Boolean) {
            numberValue = new TapBooleanValue((Boolean) value);
        }

        return numberValue;
    }
}
