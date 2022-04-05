package io.tapdata.entity.schema.value;
import io.tapdata.entity.schema.type.TapDate;

public class TapDateValue extends TapValue<DateTime, TapDate> {
    public TapDateValue() {}
    public TapDateValue(DateTime dateTime) {
        value = dateTime;
    }
}
