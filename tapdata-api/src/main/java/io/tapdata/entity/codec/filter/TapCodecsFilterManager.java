package io.tapdata.entity.codec.filter;

import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.TapDefaultCodecs;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.codec.filter.impl.FirstLayerMapIterator;
import io.tapdata.entity.error.UnknownCodecException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.entity.utils.InstanceFactory;

import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.field;
import static io.tapdata.entity.simplify.TapSimplify.tapRaw;

public class TapCodecsFilterManager {
    private static final String TAG = TapCodecsFilterManager.class.getSimpleName();
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

                String dataType = null;
                TapType typeFromSchema = null;
                ToTapValueCodec<?> valueCodec = null;
                if(nameFieldMap != null) {
                    valueCodec = this.codecsRegistry.getCustomToTapValueCodec(theValue.getClass());

                    TapField field = nameFieldMap.get(fieldName);
                    if(field != null) {
                        dataType = field.getDataType();
                        typeFromSchema = field.getTapType();
                        if(typeFromSchema != null && valueCodec == null)
                            valueCodec = typeFromSchema.toTapValueCodec();
                    }
                }
                boolean handleByTypeCodec = false;
                if(valueCodec == null) {
                    valueCodec = this.codecsRegistry.getToTapValueCodec(theValue.getClass());
                    handleByTypeCodec = true;
                }
//                if(valueCodec == null)
//                    throw new UnknownCodecException("toTapValueMap codec not found for value class " + theValue.getClass());
                if(valueCodec != null) {
                    TapValue tapValue = valueCodec.toTapValue(theValue, typeFromSchema);
                    if(tapValue == null && !handleByTypeCodec) {
                        TapLogger.warn(TAG, "Value Codec {} from model convert TapValue failed, value {}", valueCodec.getClass().getSimpleName(), theValue);
                        valueCodec = this.codecsRegistry.getToTapValueCodec(theValue.getClass());
                        if(valueCodec != null) {
                            tapValue = valueCodec.toTapValue(theValue, typeFromSchema);
                            if(tapValue == null) {
                                TapLogger.warn(TAG, "Value Codec {} from type convert TapValue failed, value {}", valueCodec.getClass().getSimpleName(), theValue);
                            }
                        }
                    }
                    if(tapValue == null) {
                        tapValue = InstanceFactory.instance(ToTapValueCodec.class, TapDefaultCodecs.TAP_RAW_VALUE)
                                .toTapValue(theValue, typeFromSchema);
                    }
                    tapValue.setOriginType(dataType);
                    if(typeFromSchema == null)
                        typeFromSchema = tapValue.createDefaultTapType();
                    tapValue.setTapType(typeFromSchema);
                    tapValue.setOriginValue(theValue);
                    entry.setValue(tapValue);
                }
            }
        });
    }

    public Map<String, TapField> transformFromTapValueMap(Map<String, Object> tapValueMap) {
        return transformFromTapValueMap(tapValueMap, null);
    }

    public Map<String, TapField> transformFromTapValueMap(Map<String, Object> tapValueMap, Map<String, TapField> sourceNameFieldMap) {
        Map<String, TapField> nameFieldMap = sourceNameFieldMap != null ? sourceNameFieldMap : new LinkedHashMap<>();
        mapIterator.iterate(tapValueMap, stringTapValueEntry -> {
            Object object = stringTapValueEntry.getValue();
            if(object instanceof TapValue) {
                TapValue<?, ?> theValue = (TapValue<?, ?>) object;
                String fieldName = stringTapValueEntry.getKey();
                if(fieldName != null) {
                    FromTapValueCodec<TapValue<?, ?>> fromTapValueCodec = this.codecsRegistry.getFromTapValueCodec((Class<TapValue<?, ?>>) theValue.getClass());
                    if(fromTapValueCodec == null)
                        throw new UnknownCodecException("fromTapValueMap codecs not found for value class " + theValue.getClass());

                    stringTapValueEntry.setValue(fromTapValueCodec.fromTapValue(theValue));
                    if(!nameFieldMap.containsKey(stringTapValueEntry.getKey())) {
                        //Handle inserted new field
                        nameFieldMap.put(stringTapValueEntry.getKey(), field(stringTapValueEntry.getKey(), theValue.getOriginType()).tapType(theValue.getTapType()));
                    }
                    //TODO Handle updated tapType field?
                    //TODO Handle deleted field?
                }
            }
        });
        return nameFieldMap;
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
