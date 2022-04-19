package io.tapdata.entity.schema.type;

import static io.tapdata.entity.simplify.TapSimplify.tapBinary;

public class TapBinary extends TapType {
    /**
     * 字段的字节长度最大值
     */
    private Long bytes;
    public TapBinary bytes(Long length) {
        this.bytes = length;
        return this;
    }

    public Long getBytes() {
        return bytes;
    }

    public void setBytes(Long bytes) {
        this.bytes = bytes;
    }

    @Override
    public TapType cloneTapType() {
        return tapBinary().bytes(bytes);
    }
}
