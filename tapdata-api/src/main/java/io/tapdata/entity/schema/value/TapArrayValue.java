package io.tapdata.entity.schema.value;

import io.tapdata.entity.schema.type.TapArray;
import io.tapdata.entity.schema.type.TapType;

import java.util.List;

public class TapArrayValue extends TapValue<List<?>, TapArray> {
    public TapArrayValue() {}
    public TapArrayValue(List<?> value) {
        this.value = value;
    }

    @Override
    public TapType createDefaultTapType() {
        return new TapArray();
    }

    @Override
    public Class<? extends TapType> tapTypeClass() {
        return TapArray.class;
    }
}
