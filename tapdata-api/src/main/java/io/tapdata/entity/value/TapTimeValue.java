package io.tapdata.entity.value;

import io.tapdata.entity.type.TapTime;

public class TapTimeValue extends TapValue<DateTime, TapTime> {
    public TapTimeValue() {}
    public TapTimeValue(DateTime dateTime) {
        value = dateTime;
    }

}
