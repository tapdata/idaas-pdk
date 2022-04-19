package io.tapdata.entity.schema.type;

import static io.tapdata.entity.simplify.TapSimplify.tapBoolean;

public class TapBoolean extends TapType {
    @Override
    public TapType cloneTapType() {
        return tapBoolean();
    }
}
