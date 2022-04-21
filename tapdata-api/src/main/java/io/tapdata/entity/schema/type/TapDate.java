package io.tapdata.entity.schema.type;

import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.entity.schema.value.TapValue;

import static io.tapdata.entity.simplify.TapSimplify.tapDate;

public class TapDate extends TapType {
    @Override
    public TapType cloneTapType() {
        return tapDate();
    }

    @Override
    public Class<? extends TapValue<?, ?>> getTapValueClass() {
        return TapDateValue.class;
    }
}
