package io.tapdata.entity.value;
import io.tapdata.entity.type.TapDate;

public class TapDateValue extends TapValue<DateTime, TapDate> {
    public TapDateValue() {}
    public TapDateValue(DateTime dateTime) {
        value = dateTime;
    }
}
