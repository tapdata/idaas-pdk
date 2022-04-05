package io.tapdata.entity.schema.value;

import io.tapdata.entity.schema.type.TapString;

public class TapStringValue extends TapValue<String, TapString> {
    public TapStringValue() {}

    public TapStringValue(String value) {
        this.value = value;
    }
}
