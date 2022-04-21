package io.tapdata.entity.conversion;

import io.tapdata.entity.codec.TapCodecRegistry;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapType;

public interface UnsupportedTypeFallbackHandler {
    void handle(TapCodecRegistry codecRegistry, TapField unsupportedField, String originType, TapType toTapType);
}
