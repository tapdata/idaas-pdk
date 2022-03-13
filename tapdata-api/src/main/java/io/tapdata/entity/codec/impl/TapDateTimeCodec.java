package io.tapdata.entity.codec.impl;

import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.type.TapDateTime;
import io.tapdata.entity.type.TapType;
import io.tapdata.entity.value.DateTime;
import io.tapdata.entity.value.TapDateTimeValue;

import java.util.Date;

public class TapDateTimeCodec implements ToTapValueCodec<TapDateTimeValue>, FromTapValueCodec<TapDateTimeValue> {
    @Override
    public Object fromTapValue(TapDateTimeValue tapValue) {
        if(tapValue == null)
            return null;
        //TODO need more code
        return tapValue.getValue();
    }

    @Override
    public TapDateTimeValue toTapValue(Object value, String originType, TapType typeFromSchema) {
        if(value == null)
            return null;
        TapType tapType;
        if(typeFromSchema instanceof TapDateTime) {
            tapType = typeFromSchema;
        } else {
            //type is not found in schema
            //type is not expected as schema wanted. type and value will be reserved
            tapType = new TapDateTime();
        }
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
            TapDateTimeValue dateTimeValue = new TapDateTimeValue(dateTime);
            dateTimeValue.setTapType((TapDateTime) tapType);
            dateTimeValue.setOriginValue(value);
            dateTimeValue.setOriginType(originType);
            return dateTimeValue;
        }
        return null;
    }
}
