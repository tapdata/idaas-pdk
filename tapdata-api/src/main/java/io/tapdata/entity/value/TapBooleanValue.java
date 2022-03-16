package io.tapdata.entity.value;
import io.tapdata.entity.type.TapBoolean;

public class TapBooleanValue extends TapValue<Boolean, TapBoolean> {
    public TapBooleanValue() {}
    public TapBooleanValue(Boolean bool) {
        value = bool;
    }
}
