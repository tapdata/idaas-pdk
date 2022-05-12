package io.tapdata.entity.codec.impl;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.codec.TapDefaultCodecs;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.TapArrayValue;

import java.util.List;

@Implementation(value = ToTapValueCodec.class, type = TapDefaultCodecs.TAP_ARRAY_VALUE, buildNumber = 0)
public class ToTapArrayCodec implements ToTapValueCodec<TapArrayValue> {

    @Override
    public TapArrayValue toTapValue(Object value, TapType typeFromSchema) {

        TapArrayValue arrayValue = null;
        if (List.class.isAssignableFrom(value.getClass())) {
            arrayValue = new TapArrayValue((List<?>) value);
        }

        return arrayValue;
    }
}
