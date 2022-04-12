package io.tapdata.entity.codec.filter;

import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.TapCodecRegistry;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.codec.filter.impl.AllLayerMapIterator;
import io.tapdata.entity.codec.filter.impl.FirstLayerMapIterator;
import io.tapdata.entity.error.UnknownCodecException;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.TapValue;

import java.util.Map;

public class TapCodecFilterManager {
    private MapIterator mapIterator;
    private final TapCodecRegistry codecRegistry;

    public TapCodecFilterManager(TapCodecRegistry codecRegistry) {
        this.codecRegistry = codecRegistry;
//        mapIterator = new AllLayerMapIterator();
        mapIterator = new FirstLayerMapIterator();
    }

    public void transformToTapValueMap(Map<String, Object> value, Map<String, TapField> nameFieldMap) {
        if(value == null)
            return;
        mapIterator.iterate(value, entry -> {
            Object theValue = entry.getValue();
            String fieldName = entry.getKey();
            if(theValue != null && fieldName != null) {
                ToTapValueCodec<?> valueCodec = this.codecRegistry.getToTapValueCodec(theValue.getClass());
//                if(valueCodec == null)
//                    throw new UnknownCodecException("toTapValueMap codec not found for value class " + theValue.getClass());
                if(valueCodec != null) {
                    String originType = null;
                    TapType typeFromSchema = null;
                    if(nameFieldMap != null) {
                        TapField field = nameFieldMap.get(fieldName);
                        if(field != null) {
                            originType = field.getOriginType();
                            typeFromSchema = field.getTapType();
                        }
                    }
                    TapValue tapValue = valueCodec.toTapValue(theValue);
                    tapValue.setOriginType(originType);
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
                    FromTapValueCodec<TapValue<?, ?>> fromTapValueCodec = this.codecRegistry.getFromTapValueCodec((Class<TapValue<?, ?>>) theValue.getClass());
                    if(fromTapValueCodec == null)
                        throw new UnknownCodecException("fromTapValueMap codec not found for value class " + theValue.getClass());

                    stringTapValueEntry.setValue(fromTapValueCodec.fromTapValue(theValue));
                }
            }
        });
    }

    public String getOriginTypeByTapType(Class<? extends TapType> tapTypeClass) {
        return codecRegistry.getOriginTypeByTapType(tapTypeClass);
    }

    public MapIterator getMapIterator() {
        return mapIterator;
    }

    public void setMapIterator(MapIterator mapIterator) {
        this.mapIterator = mapIterator;
    }
}
