package io.tapdata.entity.schema.value;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.type.TapType;


public class TapNumberValue extends TapValue<Double, TapNumber> {
    public TapNumberValue() {}
    public TapNumberValue(Double value) {
        this.value = value;
    }

    @Override
    public TapType createDefaultTapType() {
        return new TapNumber();
    }
}
