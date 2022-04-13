package io.tapdata.entity.schema.value;
import io.tapdata.entity.schema.type.TapBinary;
import io.tapdata.entity.schema.type.TapType;

public class TapBinaryValue extends TapValue<byte[], TapBinary> {
    public TapBinaryValue() {}
    public TapBinaryValue(byte[] value) {
        this.value = value;
    }

    @Override
    public TapType createDefaultTapType() {
        return new TapBinary();
    }
}
