package io.tapdata.entity.codec.impl;

import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.type.TapArray;
import io.tapdata.entity.type.TapType;
import io.tapdata.entity.value.TapArrayValue;

import java.util.List;

public class TapArrayCodec implements ToTapValueCodec<TapArrayValue>, FromTapValueCodec<TapArrayValue> {
    @Override
    public Object fromTapValue(TapArrayValue tapValue) {
        if(tapValue == null)
            return null;
        //TODO need more code
        return tapValue.getValue();
    }

    @Override
    public TapArrayValue toTapValue(Object value) {

        TapArrayValue arrayValue = null;
        if (List.class.isAssignableFrom(value.getClass())) {
            arrayValue = new TapArrayValue((List<?>) value);
        }

        return arrayValue;
    }
}
