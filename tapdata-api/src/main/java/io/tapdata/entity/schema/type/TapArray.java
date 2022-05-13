package io.tapdata.entity.schema.type;

import io.tapdata.entity.schema.value.TapArrayValue;
import io.tapdata.entity.schema.value.TapValue;

import static io.tapdata.entity.simplify.TapSimplify.tapArray;

public class TapArray extends TapType {
    public TapArray() {
        type = TYPE_ARRAY;
    }
    @Override
    public TapType cloneTapType() {
        return tapArray();
    }

    @Override
    public Class<? extends TapValue<?, ?>> tapValueClass() {
        return TapArrayValue.class;
    }
}
