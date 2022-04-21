package io.tapdata.entity.schema.type;

import io.tapdata.entity.schema.value.TapTimeValue;
import io.tapdata.entity.schema.value.TapValue;

import static io.tapdata.entity.simplify.TapSimplify.tapTime;

public class TapTime extends TapType {
    /**
     * 3（毫秒）， 6（微秒）， 9（纳秒）
     */
    private Integer scale;
    public TapTime scale(Integer scale) {
        this.scale = scale;
        return this;
    }

    public Integer getScale() {
        return scale;
    }

    public void setScale(Integer scale) {
        this.scale = scale;
    }

    @Override
    public TapType cloneTapType() {
        return tapTime().scale(scale);
    }

    @Override
    public Class<? extends TapValue<?, ?>> getTapValueClass() {
        return TapTimeValue.class;
    }
}
