package io.tapdata.entity.codec.impl;

import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.type.TapRaw;
import io.tapdata.entity.type.TapType;
import io.tapdata.entity.value.TapRawValue;

public class TapRawCodec implements ToTapValueCodec<TapRawValue>, FromTapValueCodec<TapRawValue> {
    @Override
    public Object fromTapValue(TapRawValue tapValue) {
        if(tapValue == null)
            return null;
        //TODO need more code
        return tapValue.getValue();
    }

    @Override
    public TapRawValue toTapValue(Object value) {
        if(value == null)
            return null;

        TapRawValue arrayValue = new TapRawValue(value);

        return arrayValue;
    }
}
