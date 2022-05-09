package io.tapdata.entity.schema.type;

import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.entity.schema.value.TapValue;

import static io.tapdata.entity.simplify.TapSimplify.tapDate;

public class TapDate extends TapType {
    /**
     * 字段是否有时区信息
     */
    private Boolean hasTimeZone;
    public TapDate hasTimeZone(Boolean hasTimeZone) {
        this.hasTimeZone = hasTimeZone;
        return this;
    }

    @Override
    public TapType cloneTapType() {
        return tapDate();
    }

    @Override
    public Class<? extends TapValue<?, ?>> getTapValueClass() {
        return TapDateValue.class;
    }
}
