package io.tapdata.entity.codec.impl;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.codec.TapDefaultCodecs;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapDateTimeValue;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Date;

@Implementation(value = ToTapValueCodec.class, type = TapDefaultCodecs.TAP_DATE_TIME_VALUE, buildNumber = 0)
public class ToTapDateTimeCodec implements ToTapValueCodec<TapDateTimeValue> {
    @Override
    public TapDateTimeValue toTapValue(Object value, TapType typeFromSchema) {

        DateTime dateTime = null;
        if(value instanceof DateTime) {
            dateTime = (DateTime) value;
        } else if(value instanceof Date) {
            Date date = (Date) value;
            dateTime = new DateTime();
            dateTime.setNano((int) ((date.getTime() % 1000) * 1000 * 1000));
            dateTime.setSeconds(date.getTime() / 1000);
        } /*else if(value instanceof LocalDateTime) {

        } else if(value instanceof Timestamp) {

        } else {

        }*/

        if(dateTime != null) {
            TapDateTimeValue dateTimeValue = new TapDateTimeValue(dateTime);
            return dateTimeValue;
        }
        return null;
    }
}
