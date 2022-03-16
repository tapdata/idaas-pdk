package io.tapdata.entity.value;
import io.tapdata.entity.type.TapDateTime;

public class TapDateTimeValue extends TapValue<DateTime, TapDateTime> {
    public TapDateTimeValue() {}
    public TapDateTimeValue(DateTime dateTime) {
        value = dateTime;
    }
}
