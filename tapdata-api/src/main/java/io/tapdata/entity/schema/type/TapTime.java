package io.tapdata.entity.schema.type;

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
}
