package io.tapdata.entity.type;

public class TapBinary extends TapType {
    /**
     * 字段的字节长度最大值
     */
    private Long length;

    public Long getLength() {
        return length;
    }

    public void setLength(Long length) {
        this.length = length;
    }
}