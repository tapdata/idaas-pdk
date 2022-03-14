package io.tapdata.entity.codec.impl;

import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.type.TapMap;
import io.tapdata.entity.type.TapType;
import io.tapdata.entity.value.TapMapValue;

import java.util.Map;

public class TapMapCodec implements ToTapValueCodec<TapMapValue>, FromTapValueCodec<TapMapValue> {
    @Override
    public Object fromTapValue(TapMapValue tapValue) {
        if(tapValue == null)
            return null;
        //TODO need more code
        return tapValue.getValue();
    }

    @Override
    public TapMapValue toTapValue(Object value) {
        if(value == null)
            return null;

        TapMapValue arrayValue = new TapMapValue((Map<?, ?>) value);

        return arrayValue;
    }
}
