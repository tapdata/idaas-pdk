package io.tapdata.entity.value;
import io.tapdata.entity.type.TapMap;

import java.util.Map;

public class TapMapValue extends TapValue<Map<?, ?>, TapMap> {
    public TapMapValue() {}
    public TapMapValue(Map<?, ?> value) {
        this.value = value;
    }
}
