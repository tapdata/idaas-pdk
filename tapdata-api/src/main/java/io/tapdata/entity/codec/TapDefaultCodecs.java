package io.tapdata.entity.codec;

import io.tapdata.entity.codec.impl.*;
import io.tapdata.entity.value.*;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TapDefaultCodecs {
    static TapDefaultCodecs instance = new TapDefaultCodecs();

    Map<Class<?>, ToTapValueCodec<?>> classToTapValueCodecMap = new ConcurrentHashMap<>();
    Map<Class<? extends TapValue<?, ?>>, FromTapValueCodec<?>> classFromTapValueCodecMap = new ConcurrentHashMap<>();

    private final TapArrayCodec arrayCodec = new TapArrayCodec();
    private final TapBinaryCodec binaryCodec = new TapBinaryCodec();
    private final TapBooleanCodec booleanCodec = new TapBooleanCodec();
    private final TapDateCodec dateCodec = new TapDateCodec();
    private final TapDateTimeCodec dateTimeCodec = new TapDateTimeCodec();
    private final TapMapCodec mapCodec = new TapMapCodec();
    private final TapNumberCodec numberCodec = new TapNumberCodec();
    private final TapRawCodec rawCodec = new TapRawCodec();
    private final TapStringCodec stringCodec = new TapStringCodec();
    private final TapTimeCodec timeCodec = new TapTimeCodec();

    public TapDefaultCodecs() {
        classToTapValueCodecMap.put(byte[].class, binaryCodec);
        classToTapValueCodecMap.put(boolean.class, booleanCodec);
        classToTapValueCodecMap.put(Boolean.class, booleanCodec);
        classToTapValueCodecMap.put(double.class, numberCodec);
        classToTapValueCodecMap.put(float.class, numberCodec);
        classToTapValueCodecMap.put(long.class, numberCodec);
        classToTapValueCodecMap.put(int.class, numberCodec);
        classToTapValueCodecMap.put(short.class, numberCodec);
        classToTapValueCodecMap.put(byte.class, numberCodec);
        classToTapValueCodecMap.put(String.class, stringCodec);
        classToTapValueCodecMap.put(Date.class, dateTimeCodec);

        classFromTapValueCodecMap.put(TapArrayValue.class, arrayCodec);
        classFromTapValueCodecMap.put(TapBinaryValue.class, binaryCodec);
        classFromTapValueCodecMap.put(TapBooleanValue.class, booleanCodec);
        classFromTapValueCodecMap.put(TapDateValue.class, dateCodec);
        classFromTapValueCodecMap.put(TapDateTimeValue.class, dateTimeCodec);
        classFromTapValueCodecMap.put(TapMapValue.class, mapCodec);
        classFromTapValueCodecMap.put(TapNumberValue.class, numberCodec);
        classFromTapValueCodecMap.put(TapRawValue.class, rawCodec);
        classFromTapValueCodecMap.put(TapStringValue.class, stringCodec);
        classFromTapValueCodecMap.put(TapTimeValue.class, timeCodec);
    }

    ToTapValueCodec<?> getToTapValueCodec(Class<?> clazz) {
        ToTapValueCodec<?> codec = classToTapValueCodecMap.get(clazz);
        if(codec == null) {
            if(Collection.class.isAssignableFrom(clazz)) {
                return arrayCodec;
            } else if(Map.class.isAssignableFrom(clazz)) {
                return mapCodec;
            } else if(Number.class.isAssignableFrom(clazz)) {
                return numberCodec;
            }
        }
        return rawCodec;
    }

    FromTapValueCodec<?> getFromTapValueCodec(Class<? extends TapValue<?, ?>> clazz) {
        return classFromTapValueCodecMap.get(clazz);
    }

    public static void main(String[] args) {
        System.out.println("bool " + Number.class.isAssignableFrom(BigDecimal.class));
    }
}
