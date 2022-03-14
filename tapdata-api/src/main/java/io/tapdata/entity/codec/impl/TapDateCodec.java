package io.tapdata.entity.codec.impl;

import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.type.TapDate;
import io.tapdata.entity.type.TapType;
import io.tapdata.entity.value.DateTime;
import io.tapdata.entity.value.TapDateValue;

import java.util.Date;

public class TapDateCodec implements ToTapValueCodec<TapDateValue>, FromTapValueCodec<TapDateValue> {
    @Override
    public Object fromTapValue(TapDateValue tapValue) {
        if(tapValue == null)
            return null;
        //TODO need more code
        return tapValue.getValue();
    }

    @Override
    public TapDateValue toTapValue(Object value) {
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
        TapDateValue tapDateValue = null;
        if(dateTime != null) {
            tapDateValue = new TapDateValue(dateTime);
        }

        return tapDateValue;
    }
}
