package io.tapdata.entity.schema.value;
import io.tapdata.entity.schema.type.TapMap;

import java.util.Map;

public class TapMapValue extends TapValue<Map<?, ?>, TapMap> {
    public TapMapValue() {}
    public TapMapValue(Map<?, ?> value) {
        this.value = value;
    }
}
