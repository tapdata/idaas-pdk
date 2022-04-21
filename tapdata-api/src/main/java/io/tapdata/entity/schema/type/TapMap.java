package io.tapdata.entity.schema.type;

import io.tapdata.entity.schema.value.TapMapValue;
import io.tapdata.entity.schema.value.TapValue;

import static io.tapdata.entity.simplify.TapSimplify.tapMap;

public class TapMap extends TapType {
    @Override
    public TapType cloneTapType() {
        return tapMap();
    }

    @Override
    public Class<? extends TapValue<?, ?>> getTapValueClass() {
        return TapMapValue.class;
    }
}
