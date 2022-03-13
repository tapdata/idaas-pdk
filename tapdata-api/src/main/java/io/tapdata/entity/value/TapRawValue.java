package io.tapdata.entity.value;

import io.tapdata.entity.type.TapRaw;

public class TapRawValue extends TapValue<Object, TapRaw> {
    public TapRawValue() {}
    public TapRawValue(Object value) {
        this.value = value;
    }

}
