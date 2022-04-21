package io.tapdata.entity.schema.type;

import io.tapdata.entity.schema.value.TapBooleanValue;
import io.tapdata.entity.schema.value.TapValue;

import static io.tapdata.entity.simplify.TapSimplify.tapBoolean;

public class TapBoolean extends TapType {
    @Override
    public TapType cloneTapType() {
        return tapBoolean();
    }

    @Override
    public Class<? extends TapValue<?, ?>> getTapValueClass() {
        return TapBooleanValue.class;
    }
}
