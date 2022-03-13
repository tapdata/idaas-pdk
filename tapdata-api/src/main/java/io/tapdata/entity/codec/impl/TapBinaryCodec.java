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
    public TapBinaryValue toTapValue(Object value, String originType, TapType typeFromSchema) {
        if(value == null)
            return null;
        TapType tapType;
        if(typeFromSchema instanceof TapBinary) {
            tapType = typeFromSchema;
        } else {
            //type is not found in schema
            //type is not expected as schema wanted. type and value will be reserved
            tapType = new TapBinary();
        }
        TapBinaryValue arrayValue = new TapBinaryValue((byte[]) value);
        arrayValue.setTapType((TapBinary) tapType);
        arrayValue.setOriginValue(value);
        arrayValue.setOriginType(originType);
        return arrayValue;
    }
}
