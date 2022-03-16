package io.tapdata.entity.value;

import io.tapdata.entity.type.TapArray;

import java.util.List;

public class TapArrayValue extends TapValue<List<?>, TapArray> {
    public TapArrayValue() {}
    public TapArrayValue(List<?> value) {
        this.value = value;
    }
}
