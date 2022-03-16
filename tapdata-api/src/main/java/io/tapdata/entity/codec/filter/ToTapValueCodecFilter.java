package io.tapdata.entity.codec.filter;

import io.tapdata.entity.value.TapValue;

import java.util.Map;

public interface ToTapValueCodecFilter {
    Map<String, TapValue<?, ?>> filter(Map<String, Object> value);
}
