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
    public void handle(TapCodecRegistry codecsRegistry, TapField unsupportedField, String dataType, TapType toTapType) {
        if(codecsRegistry != null) {
            TapType unsupportedTapType = unsupportedField.getTapType();
            if(unsupportedTapType != null && !codecsRegistry.isRegisteredFromTapValue(unsupportedTapType.getTapValueClass())) {
                codecsRegistry.registerFromTapValue(unsupportedTapType.getTapValueClass(), dataType, tapValue -> {
                    Object value = tapValue.getValue();
                    return value.toString();
                });
            }

        }
    }
}
