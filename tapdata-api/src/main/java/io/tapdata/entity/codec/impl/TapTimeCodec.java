package io.tapdata.entity.codec.impl;

import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.type.TapTime;
import io.tapdata.entity.type.TapType;
import io.tapdata.entity.value.DateTime;
import io.tapdata.entity.value.TapTimeValue;

import java.util.Date;

public class TapTimeCodec implements ToTapValueCodec<TapTimeValue>, FromTapValueCodec<TapTimeValue> {
    @Override
    public Object fromTapValue(TapTimeValue tapValue) {
        if(tapValue == null)
            return null;
        //TODO need more code
        return tapValue.getValue();
    }

    @Override
    public TapTimeValue toTapValue(Object value) {
        if(value == null)
            return null;

        DateTime dateTime = null;
        if(value instanceof DateTime) {
            dateTime = (DateTime) value;
        } else if(value instanceof Date) {
            Date date = (Date) value;
            dateTime = new DateTime();
            dateTime.setNano(date.getTime() * 1000 * 1000);
            dateTime.setSeconds(date.getTime() / 1000);
        }

        if(dateTime != null) {
            TapTimeValue dateTimeValue = new TapTimeValue(dateTime);
            return dateTimeValue;
        }
        return null;
    }
}
