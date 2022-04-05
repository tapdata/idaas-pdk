package io.tapdata.entity.schema.value;

import io.tapdata.entity.schema.type.TapTime;

public class TapTimeValue extends TapValue<DateTime, TapTime> {
    public TapTimeValue() {}
    public TapTimeValue(DateTime dateTime) {
        value = dateTime;
    }

}
