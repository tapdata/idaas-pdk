package io.tapdata.entity.type;

public class TapString extends TapType {
    public TapString() {}
    public TapString(Long length, Boolean fixed) {
        this.length = length;
        this.fixed = fixed;
    }
    /**
     * 字段类型的长度最大值， VARCHAR(100), 只支持100长度的字符串
     */
    private Long length;
    /**
     * 字段长度是否固定， 写一个字符， 补齐99个空字符的问题
     */
    private Boolean fixed;

    public Long getLength() {
        return length;
    }

    public void setLength(Long length) {
        this.length = length;
    }

    public Boolean getFixed() {
        return fixed;
    }

    public void setFixed(Boolean fixed) {
        this.fixed = fixed;
    }
}
