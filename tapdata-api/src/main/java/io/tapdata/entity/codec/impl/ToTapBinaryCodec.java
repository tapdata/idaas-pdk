package io.tapdata.entity.codec.impl;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.codec.TapDefaultCodecs;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.value.TapBinaryValue;

@Implementation(value = ToTapValueCodec.class, type = TapDefaultCodecs.TAP_BINARY_VALUE, buildNumber = 0)
public class ToTapBinaryCodec implements ToTapValueCodec<TapBinaryValue> {
    @Override
    public TapBinaryValue toTapValue(Object value) {

        TapBinaryValue arrayValue = new TapBinaryValue((byte[]) value);

        return arrayValue;
    }
}
