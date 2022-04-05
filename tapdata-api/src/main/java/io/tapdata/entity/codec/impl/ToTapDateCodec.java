package io.tapdata.entity.codec.impl;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.codec.TapDefaultCodecs;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapDateValue;

import java.util.Date;

@Implementation(value = ToTapValueCodec.class, type = TapDefaultCodecs.TAP_DATE_VALUE, buildNumber = 0)
public class ToTapDateCodec implements ToTapValueCodec<TapDateValue> {
    @Override
    public TapDateValue toTapValue(Object value) {

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
