package io.tapdata.entity.schema.type;

import io.tapdata.entity.schema.value.TapBooleanValue;
import io.tapdata.entity.schema.value.TapValue;

import static io.tapdata.entity.simplify.TapSimplify.tapBoolean;

public class TapBoolean extends TapType {
    public TapBoolean() {
        type = TYPE_BOOLEAN;
    }
    @Override
    public TapType cloneTapType() {
        return tapBoolean();
    }

    @Override
    public Class<? extends TapValue<?, ?>> tapValueClass() {
        return TapBooleanValue.class;
    }
}
