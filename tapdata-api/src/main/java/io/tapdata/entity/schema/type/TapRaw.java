package io.tapdata.entity.schema.type;

import static io.tapdata.entity.simplify.TapSimplify.tapRaw;

/**
 * 不是为了数据库写入， 而是我们在内存里写， 带到目标库。
 * 在完全不认识的字段时， 会有用户， 例如同构时， 可以使用这个在字段到目标库， 写入， 之后originValue， 都可以没有value
 */
public class TapRaw extends TapType {
    @Override
    public TapType cloneTapType() {
        return tapRaw();
    }
}
