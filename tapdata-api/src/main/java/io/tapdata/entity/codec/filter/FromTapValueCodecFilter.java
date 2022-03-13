package io.tapdata.entity.codec.filter;

import io.tapdata.entity.value.TapValue;

import java.util.Map;

public interface FromTapValueCodecFilter {
    Map<String, Object> filter(Map<String, TapValue<?, ?>> tapValueMap);
}
