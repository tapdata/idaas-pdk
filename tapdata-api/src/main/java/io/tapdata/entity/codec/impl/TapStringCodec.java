package io.tapdata.entity.codec.impl;

import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.type.TapType;
import io.tapdata.entity.value.TapStringValue;

import java.util.*;

public class TapStringCodec implements FromTapValueCodec<TapStringValue>, ToTapValueCodec<TapStringValue> {
    @Override
    public Object fromTapValue(TapStringValue tapValue) {
        if(tapValue == null)
            return null;
        return tapValue.getValue();
    }

    @Override
    public TapStringValue toTapValue(Object value, String originType, TapType typeFromSchema) {
        if(value == null)
            return null;
        TapStringValue stringValue;
        Class<?> clazz = value.getClass();
        if(value instanceof String) {
            stringValue = new TapStringValue((String) value);
        } /*else if(CodecUtils.isPrimitiveOrWrapper(clazz)) {
            stringValue = new TapStringValue(value.toString());
        }*/ else if (Collection.class.isAssignableFrom(clazz)) {
            Collection<?> collection = (Collection<?>) value;
            stringValue = new TapStringValue(Arrays.toString(collection.toArray()));
        } else if (Map.class.isAssignableFrom(clazz)) {
            Map<?, ?> map = (Map<?, ?>) value;
            stringValue = new TapStringValue(Arrays.toString(map.entrySet().toArray()));
        } else {
            stringValue = new TapStringValue(value.toString());
        }
        return stringValue;
    }
}
