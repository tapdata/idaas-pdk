package io.tapdata.entity.schema.type;

import io.tapdata.entity.schema.value.TapStringValue;
import io.tapdata.entity.schema.value.TapValue;

import static io.tapdata.entity.simplify.TapSimplify.tapString;

public class TapString extends TapType {
    public TapString() {}
    public TapString(Long bytes, Boolean fixed) {
        this.bytes = bytes;
        this.fixed = fixed;
    }
    /**
     * 字段类型的长度最大值， VARCHAR(100), 只支持100长度的字符串
     */
    private Long bytes;
    public TapString bytes(Long bytes) {
        this.bytes = bytes;
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

    public Long getBytes() {
        return bytes;
    }

    public void setBytes(Long bytes) {
        this.bytes = bytes;
    }

    public Boolean getFixed() {
        return fixed;
    }

    public void setFixed(Boolean fixed) {
        this.fixed = fixed;
    }

    @Override
    public TapType cloneTapType() {
        return tapString().fixed(fixed).bytes(bytes);
    }

    @Override
    public Class<? extends TapValue<?, ?>> getTapValueClass() {
        return TapStringValue.class;
    }
}
