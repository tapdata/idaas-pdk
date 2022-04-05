package io.tapdata.entity.schema.value;
import io.tapdata.entity.schema.type.TapDateTime;

public class TapDateTimeValue extends TapValue<DateTime, TapDateTime> {
    public TapDateTimeValue() {}
    public TapDateTimeValue(DateTime dateTime) {
        value = dateTime;
    }
}
