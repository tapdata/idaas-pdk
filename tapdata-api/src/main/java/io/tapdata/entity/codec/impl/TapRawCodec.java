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
    public TapRawValue toTapValue(Object value, String originType, TapType typeFromSchema) {
        if(value == null)
            return null;
        TapType tapType;
        if(typeFromSchema instanceof TapRaw) {
            tapType = typeFromSchema;
        } else {
            //type is not found in schema
            //type is not expected as schema wanted. type and value will be reserved
            tapType = new TapRaw();
        }
        TapRawValue arrayValue = new TapRawValue(value);
        arrayValue.setTapType((TapRaw) tapType);
        arrayValue.setOriginValue(value);
        arrayValue.setOriginType(originType);
        return arrayValue;
    }
}
