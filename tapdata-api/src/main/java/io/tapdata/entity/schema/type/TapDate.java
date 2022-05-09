package io.tapdata.entity.schema.type;

import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.entity.schema.value.TapValue;

import java.util.Date;

import static io.tapdata.entity.simplify.TapSimplify.tapDate;

public class TapDate extends TapType {
    /**
     * 字段是否有时区信息
     */
    private Boolean withTimeZone;
    public TapDate withTimeZone(Boolean withTimeZone) {
        this.withTimeZone = withTimeZone;
        return this;
    }

    private Long bytes;
    private Date min;
    private Date max;
    private Integer fraction;
    private Integer defaultFraction;

    @Override
    public TapType cloneTapType() {
        return tapDate();
    }

    @Override
    public Class<? extends TapValue<?, ?>> getTapValueClass() {
        return TapDateValue.class;
    }
}
