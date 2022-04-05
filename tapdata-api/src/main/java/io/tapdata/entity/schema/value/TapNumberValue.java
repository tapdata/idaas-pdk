package io.tapdata.entity.schema.value;
import io.tapdata.entity.schema.type.TapNumber;


public class TapNumberValue extends TapValue<Double, TapNumber> {
    public TapNumberValue() {}
    public TapNumberValue(Double value) {
        this.value = value;
    }
}
