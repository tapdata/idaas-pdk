package io.tapdata.entity.codec.impl;

import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.type.TapBinary;
import io.tapdata.entity.type.TapType;
import io.tapdata.entity.value.TapBinaryValue;

public class TapBinaryCodec implements ToTapValueCodec<TapBinaryValue>, FromTapValueCodec<TapBinaryValue> {
    @Override
    public Object fromTapValue(TapBinaryValue tapValue) {
        if(tapValue == null)
            return null;
        //TODO need more code
        return tapValue.getValue();
    }

    @Override
    public TapBinaryValue toTapValue(Object value) {

        TapBinaryValue arrayValue = new TapBinaryValue((byte[]) value);

        return arrayValue;
    }
}
