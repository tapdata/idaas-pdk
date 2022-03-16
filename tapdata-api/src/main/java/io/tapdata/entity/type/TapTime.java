package io.tapdata.entity.type;

public class TapTime extends TapType {
    private Long scale;
    public TapTime scale(Long scale) {
        this.scale = scale;
        return this;
    }

    public Long getScale() {
        return scale;
    }

    public void setScale(Long scale) {
        this.scale = scale;
    }
}
