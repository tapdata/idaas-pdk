package io.tapdata.entity.schema.type;

import static io.tapdata.entity.simplify.TapSimplify.tapDateTime;

public class TapDateTime extends TapType {
    /**
     * 字段的时间精度
     * 秒之后算scale， 3（毫秒）， 6（微秒）， 9（纳秒）
     * 秒点之后算scale
     *
     */
    private Integer scale;
    public TapDateTime scale(Integer scale) {
        this.scale = scale;
        return this;
    }
    /**
     * 字段是否有时区信息
     */
    private Boolean hasTimeZone;
    public TapDateTime hasTimeZone(Boolean hasTimeZone) {
        this.hasTimeZone = hasTimeZone;
        return this;
    }

    public Integer getScale() {
        return scale;
    }

    public void setScale(Integer scale) {
        this.scale = scale;
    }

    public Boolean getHasTimeZone() {
        return hasTimeZone;
    }

    public void setHasTimeZone(Boolean hasTimeZone) {
        this.hasTimeZone = hasTimeZone;
    }

    @Override
    public TapType cloneTapType() {
        return tapDateTime().hasTimeZone(hasTimeZone).scale(scale);
    }
}
