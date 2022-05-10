package io.tapdata.entity.codec.filter;

import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.codec.filter.impl.FirstLayerMapIterator;
import io.tapdata.entity.error.UnknownCodecException;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.TapValue;

import java.util.Map;

public class TapCodecsFilterManager {
    private MapIterator mapIterator;
    private final TapCodecsRegistry codecsRegistry;

    public TapCodecsFilterManager(TapCodecsRegistry codecsRegistry) {
        this.codecsRegistry = codecsRegistry;
//        mapIterator = new AllLayerMapIterator();
        mapIterator = new FirstLayerMapIterator();
    }

    public static TapCodecsFilterManager create(TapCodecsRegistry codecsRegistry) {
        return new TapCodecsFilterManager(codecsRegistry);
    }

    public void transformToTapValueMap(Map<String, Object> value, Map<String, TapField> nameFieldMap) {
        if(value == null)
            return;
        mapIterator.iterate(value, entry -> {
            Object theValue = entry.getValue();
            String fieldName = entry.getKey();
            if(theValue != null && fieldName != null) {
                ToTapValueCodec<?> valueCodec = this.codecsRegistry.getToTapValueCodec(theValue.getClass());
//                if(valueCodec == null)
//                    throw new UnknownCodecException("toTapValueMap codec not found for value class " + theValue.getClass());
                if(valueCodec != null) {
                    String dataType = null;
                    TapType typeFromSchema = null;
                    if(nameFieldMap != null) {
                        TapField field = nameFieldMap.get(fieldName);
                        if(field != null) {
                            dataType = field.getDataType();
                            typeFromSchema = field.getTapType();
                        }
                    }
                    TapValue tapValue = valueCodec.toTapValue(theValue, typeFromSchema);
                    tapValue.setOriginType(dataType);
                    tapValue.setTapType(typeFromSchema);
                    tapValue.setOriginValue(theValue);
                    entry.setValue(tapValue);
                }
            }
        });
    }

    public void transformFromTapValueMap(Map<String, Object> tapValueMap) {
        mapIterator.iterate(tapValueMap, stringTapValueEntry -> {
            Object object = stringTapValueEntry.getValue();
            if(object instanceof TapValue) {
                TapValue<?, ?> theValue = (TapValue<?, ?>) object;
                String fieldName = stringTapValueEntry.getKey();
                if(fieldName != null) {
                    FromTapValueCodec<TapValue<?, ?>> fromTapValueCodec = this.codecsRegistry.getFromTapValueCodec((Class<TapValue<?, ?>>) theValue.getClass());
                    if(fromTapValueCodec == null)
                        throw new UnknownCodecException("fromTapValueMap codec not found for value class " + theValue.getClass());

                    stringTapValueEntry.setValue(fromTapValueCodec.fromTapValue(theValue));
                }
            }
        });
    }

    public String getDataTypeByTapType(Class<? extends TapType> tapTypeClass) {
        return codecsRegistry.getDataTypeByTapType(tapTypeClass);
    }

    public Map<Class<?>, String> getTapTypeDataTypeMap() {
        return codecsRegistry.getTapTypeDataTypeMap();
    }

    public ToTapValueCodec<?> getToTapValueCodec(Object value) {
        return this.codecsRegistry.getToTapValueCodec(value.getClass());
    }

    public MapIterator getMapIterator() {
        return mapIterator;
    }

    public void setMapIterator(MapIterator mapIterator) {
        this.mapIterator = mapIterator;
    }

    public TapCodecsRegistry getCodecsRegistry() {
        return codecsRegistry;
    }
}
