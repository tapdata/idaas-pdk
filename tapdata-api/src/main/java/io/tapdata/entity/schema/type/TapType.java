package io.tapdata.entity.schema.type;

import io.tapdata.entity.schema.value.TapValue;

import java.io.Serializable;

public abstract class TapType implements Serializable {
    public static final byte TYPE_DATETIME = 1;
    public static final byte TYPE_ARRAY = 2;
    public static final byte TYPE_BOOLEAN = 3;
    public static final byte TYPE_MAP = 4;
    public static final byte TYPE_YEAR = 5;
    public static final byte TYPE_TIME = 6;
    public static final byte TYPE_RAW = 7;
    public static final byte TYPE_NUMBER = 8;
    public static final byte TYPE_BINARY = 9;
    public static final byte TYPE_STRING = 10;
    public static final byte TYPE_DATE = 11;
    protected byte type;

    public abstract TapType cloneTapType();
    public abstract Class<? extends TapValue<?, ?>> getTapValueClass();

    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
    }
}
