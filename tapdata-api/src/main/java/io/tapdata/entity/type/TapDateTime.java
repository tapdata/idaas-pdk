package io.tapdata.entity.type;

public class TapDateTime extends TapType {
    /**
     * 字段的时间精度
     * 秒之后算scale， 3（毫秒）， 6（微秒）， 9（纳秒）
     * 秒点之后算scale
     *
     */
    private Long scale;
    public TapDateTime scale(Long scale) {
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

    public Long getScale() {
        return scale;
    }

    public void setScale(Long scale) {
        this.scale = scale;
    }

    public Boolean getHasTimeZone() {
        return hasTimeZone;
    }

    public void setHasTimeZone(Boolean hasTimeZone) {
        this.hasTimeZone = hasTimeZone;
    }
}
