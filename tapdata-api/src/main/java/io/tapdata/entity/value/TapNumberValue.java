package io.tapdata.entity.value;
import io.tapdata.entity.type.TapNumber;


public class TapNumberValue extends TapValue<Double, TapNumber> {
    public TapNumberValue() {}
    public TapNumberValue(Double value) {
        this.value = value;
    }
}
