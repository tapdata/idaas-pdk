package io.tapdata.entity.schema.type;

import io.tapdata.entity.schema.value.TapValue;

public abstract class TapType {
    public abstract TapType cloneTapType();
    public abstract Class<? extends TapValue<?, ?>> getTapValueClass();
}
