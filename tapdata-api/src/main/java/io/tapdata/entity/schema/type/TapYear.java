package io.tapdata.entity.schema.type;

import io.tapdata.entity.codec.TapDefaultCodecs;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.codec.impl.ToTapYearCodec;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.entity.schema.value.TapYearValue;
import io.tapdata.entity.utils.InstanceFactory;

import static io.tapdata.entity.simplify.TapSimplify.tapYear;

public class TapYear extends TapType {
    public TapYear() {
        type = TYPE_YEAR;
    }

    @Override
    public TapType cloneTapType() {
        return tapYear();
    }

    @Override
    public Class<? extends TapValue<?, ?>> tapValueClass() {
        return TapYearValue.class;
    }

    @Override
    public ToTapValueCodec<?> toTapValueCodec() {
        return InstanceFactory.instance(ToTapValueCodec.class, TapDefaultCodecs.TAP_YEAR_VALUE);
    }
}
