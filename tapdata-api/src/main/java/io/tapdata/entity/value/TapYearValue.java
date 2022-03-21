package io.tapdata.entity.value;

import io.tapdata.entity.type.TapYear;

public class TapYearValue extends TapValue<DateTime, TapYear> {
    public TapYearValue() {}
    public TapYearValue(DateTime dateTime) {
        value = dateTime;
    }
}
