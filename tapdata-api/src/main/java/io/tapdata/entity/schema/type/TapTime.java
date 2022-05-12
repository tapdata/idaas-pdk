package io.tapdata.entity.schema.type;

import io.tapdata.entity.schema.value.TapTimeValue;
import io.tapdata.entity.schema.value.TapValue;

import java.time.Instant;
import java.time.LocalDateTime;

import static io.tapdata.entity.simplify.TapSimplify.tapTime;

public class TapTime extends TapType {
    /**
     * 字段是否有时区信息
     */
    private Boolean withTimeZone;
    public TapTime withTimeZone(Boolean withTimeZone) {
        this.withTimeZone = withTimeZone;
        return this;
    }

    private Long bytes;
    public TapTime bytes(Long bytes) {
        this.bytes = bytes;
        return this;
    }
    private Instant min;
    public TapTime min(Instant min) {
        this.min = min;
        return this;
    }
    private Instant max;
    public TapTime max(Instant max) {
        this.max = max;
        return this;
    }

    public Boolean getWithTimeZone() {
        return withTimeZone;
    }

    public void setWithTimeZone(Boolean withTimeZone) {
        this.withTimeZone = withTimeZone;
    }

    public Long getBytes() {
        return bytes;
    }

    public void setBytes(Long bytes) {
        this.bytes = bytes;
    }

    public Instant getMin() {
        return min;
    }

    public void setMin(Instant min) {
        this.min = min;
    }

    public Instant getMax() {
        return max;
    }

    public void setMax(Instant max) {
        this.max = max;
    }

    @Override
    public TapType cloneTapType() {
        return tapTime().min(min).max(max).withTimeZone(withTimeZone).bytes(bytes);
    }

    @Override
    public Class<? extends TapValue<?, ?>> getTapValueClass() {
        return TapTimeValue.class;
    }
}
