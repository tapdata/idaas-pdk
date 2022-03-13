package io.tapdata.entity.codec;

import io.tapdata.entity.type.TapType;
import io.tapdata.entity.value.TapValue;

public interface ToTapValueCodec<T extends TapValue<?, ?>> {
    T toTapValue(Object value, String originType, TapType typeFromSchema);
}
