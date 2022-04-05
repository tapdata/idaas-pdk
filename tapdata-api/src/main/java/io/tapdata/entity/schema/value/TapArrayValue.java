package io.tapdata.entity.schema.value;

import io.tapdata.entity.schema.type.TapArray;

import java.util.List;

public class TapArrayValue extends TapValue<List<?>, TapArray> {
    public TapArrayValue() {}
    public TapArrayValue(List<?> value) {
        this.value = value;
    }
}
