package io.tapdata.entity.schema.value;

import io.tapdata.entity.schema.type.TapRaw;

public class TapRawValue extends TapValue<Object, TapRaw> {
    public TapRawValue() {}
    public TapRawValue(Object value) {
        this.value = value;
    }

}
