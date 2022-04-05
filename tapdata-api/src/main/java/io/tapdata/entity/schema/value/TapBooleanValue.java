package io.tapdata.entity.schema.value;
import io.tapdata.entity.schema.type.TapBoolean;

public class TapBooleanValue extends TapValue<Boolean, TapBoolean> {
    public TapBooleanValue() {}
    public TapBooleanValue(Boolean bool) {
        value = bool;
    }
}
