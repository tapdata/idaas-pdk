package io.tapdata.entity.schema.type;

public class TapBinary extends TapType {
    /**
     * 字段的字节长度最大值
     */
    private Long width;
    public TapBinary width(Long length) {
        this.width = length;
        return this;
    }

    public Long getWidth() {
        return width;
    }

    public void setWidth(Long width) {
        this.width = width;
    }
}
