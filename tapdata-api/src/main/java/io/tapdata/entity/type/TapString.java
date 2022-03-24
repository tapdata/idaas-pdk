package io.tapdata.entity.type;

public class TapString extends TapType {
    public TapString() {}
    public TapString(Long width, Boolean fixed) {
        this.width = width;
        this.fixed = fixed;
    }
    /**
     * 字段类型的长度最大值， VARCHAR(100), 只支持100长度的字符串
     */
    private Long width;
    public TapString width(Long width) {
        this.width = width;
        return this;
    }
    /**
     * 字段长度是否固定， 写一个字符， 补齐99个空字符的问题
     */
    private Boolean fixed;
    public TapString fixed(Boolean fixed) {
        this.fixed = fixed;
        return this;
    }

    public Long getWidth() {
        return width;
    }

    public void setWidth(Long width) {
        this.width = width;
    }

    public Boolean getFixed() {
        return fixed;
    }

    public void setFixed(Boolean fixed) {
        this.fixed = fixed;
    }
}
