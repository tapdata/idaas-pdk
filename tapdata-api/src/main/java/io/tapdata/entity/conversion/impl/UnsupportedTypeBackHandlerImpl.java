package io.tapdata.entity.conversion.impl;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.TapCodecRegistry;
import io.tapdata.entity.conversion.UnsupportedTypeFallbackHandler;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.TapValue;

@Implementation(value = UnsupportedTypeFallbackHandler.class, buildNumber = 0)
public class UnsupportedTypeBackHandlerImpl implements UnsupportedTypeFallbackHandler {
    @Override
    public void handle(TapCodecRegistry codecRegistry, TapField unsupportedField, String originType, TapType toTapType) {
        if(codecRegistry != null) {
            TapType unsupportedTapType = unsupportedField.getTapType();
            if(unsupportedTapType != null && !codecRegistry.isRegisteredFromTapValue(unsupportedTapType.getTapValueClass())) {
                codecRegistry.registerFromTapValue(unsupportedTapType.getTapValueClass(), originType, tapValue -> {
                    Object value = tapValue.getValue();
                    return value.toString();
                });
            }

        }
    }
}
