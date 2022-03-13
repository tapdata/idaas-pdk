package io.tapdata.entity.value;
import io.tapdata.entity.type.TapBinary;

public class TapBinaryValue extends TapValue<byte[], TapBinary> {
    public TapBinaryValue() {}
    public TapBinaryValue(byte[] value) {
        this.value = value;
    }
}
