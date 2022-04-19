package io.tapdata.entity.schema.type;

import static io.tapdata.entity.simplify.TapSimplify.tapArray;

public class TapArray extends TapType {
    @Override
    public TapType cloneTapType() {
        return tapArray();
    }
}
