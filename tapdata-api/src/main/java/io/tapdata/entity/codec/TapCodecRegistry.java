package io.tapdata.entity.codec;

import io.tapdata.entity.value.TapValue;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TapCodecRegistry {
    private final Map<Class<?>, ToTapValueCodec<? extends TapValue<?, ?>>> classToTapValueCodecMap = new ConcurrentHashMap<>();
    private final Map<Class<? extends TapValue<?, ?>>, FromTapValueCodec<? extends TapValue<?, ?>>> classFromTapValueCodecMap = new ConcurrentHashMap<>();

//    private final Map<String, ToTapValueCodec<?>> fieldToTapValueCodecMap = new ConcurrentHashMap<>();

    public TapCodecRegistry() {
    }

    public <T extends TapValue<?, ?>> TapCodecRegistry registerToTapValue(Class<?> anyClass, ToTapValueCodec<T> toTapValueCodec) {
        classToTapValueCodecMap.put(anyClass, toTapValueCodec);
        return this;
    }

    public void unregisterToTapValue(Class<?> anyClass) {
        classToTapValueCodecMap.remove(anyClass);
    }

    public <T extends TapValue<?, ?>> TapCodecRegistry registerFromTapValue(Class<? extends TapValue<?, ?>> tapTypeClass, FromTapValueCodec<T> fromTapValueCodec) {
        classFromTapValueCodecMap.put(tapTypeClass, fromTapValueCodec);
        return this;
    }

    public void unregisterFromTapValue(Class<? extends TapValue<?, ?>> tapTypeClass) {
        classFromTapValueCodecMap.remove(tapTypeClass);
    }

//    public <T extends TapValue<?, ?>> TapCodecRegistry registerFieldToTapValue(String fieldName, ToTapValueCodec<T> toTapValueCodec) {
//        fieldToTapValueCodecMap.put(fieldName, toTapValueCodec);
//        return this;
//    }
//
//    public void unregisterFieldToTapValue(String fieldName) {
//        fieldToTapValueCodecMap.remove(fieldName);
//    }


    public ToTapValueCodec<?> getToTapValueCodec(Class<?> clazz) {
        ToTapValueCodec<?> codec = classToTapValueCodecMap.get(clazz);
        if(codec == null) {
            codec = TapDefaultCodecs.instance.getToTapValueCodec(clazz);
        }
        return codec;
    }

    public <T extends TapValue<?, ?>> FromTapValueCodec<T> getFromTapValueCodec(Class<T> clazz) {
        FromTapValueCodec<T> codec = (FromTapValueCodec<T>) classFromTapValueCodecMap.get(clazz);
        if(codec == null) {
            codec = (FromTapValueCodec<T>) TapDefaultCodecs.instance.getFromTapValueCodec(clazz);
        }
        return codec;
    }

//    public ToTapValueCodec<?> getFieldToTapValueCodec(String fieldName) {
//        ToTapValueCodec<?> codec = fieldToTapValueCodecMap.get(fieldName);
//        return codec;
//    }
}
