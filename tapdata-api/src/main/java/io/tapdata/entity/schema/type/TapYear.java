package io.tapdata.entity.schema.type;

import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.entity.schema.value.TapYearValue;

import static io.tapdata.entity.simplify.TapSimplify.tapYear;

public class TapYear extends TapType {

    @Override
    public TapType cloneTapType() {
        return tapYear();
    }

    @Override
    public Class<? extends TapValue<?, ?>> getTapValueClass() {
        return TapYearValue.class;
    }
}
