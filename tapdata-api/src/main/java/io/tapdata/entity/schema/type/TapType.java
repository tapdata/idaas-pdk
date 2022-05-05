package io.tapdata.entity.schema.type;

import io.tapdata.entity.schema.value.TapValue;

import java.io.Serializable;

public abstract class TapType implements Serializable {
    public abstract TapType cloneTapType();
    public abstract Class<? extends TapValue<?, ?>> getTapValueClass();
}
