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
    public TapArrayValue toTapValue(Object value, String originType, TapType typeFromSchema) {
        if(value == null)
            return null;
        TapType tapType;
        if(typeFromSchema instanceof TapArray) {
            tapType = typeFromSchema;
        } else {
            //type is not found in schema
            //type is not expected as schema wanted. type and value will be reserved
            tapType = new TapArray();
        }
        TapArrayValue arrayValue = null;
        if (List.class.isAssignableFrom(value.getClass())) {
            arrayValue = new TapArrayValue((List<?>) value);
        }

        if(arrayValue != null) {
            arrayValue.setTapType((TapArray) tapType);
            arrayValue.setOriginValue(value);
            arrayValue.setOriginType(originType);
        }

        return arrayValue;
    }
}
