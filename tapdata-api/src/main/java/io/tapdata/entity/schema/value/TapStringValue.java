package io.tapdata.entity.schema.value;

import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.schema.type.TapType;

public class TapStringValue extends TapValue<String, TapString> {
    public TapStringValue() {}

    public TapStringValue(String value) {
        this.value = value;
    }

    @Override
    public TapType createDefaultTapType() {
        return new TapString();
    }
}
