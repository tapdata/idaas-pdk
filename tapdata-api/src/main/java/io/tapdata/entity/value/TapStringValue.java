package io.tapdata.entity.value;

import io.tapdata.entity.type.TapString;

public class TapStringValue extends TapValue<String, TapString> {
    public TapStringValue() {}

    public TapStringValue(String value) {
        this.value = value;
    }
}
