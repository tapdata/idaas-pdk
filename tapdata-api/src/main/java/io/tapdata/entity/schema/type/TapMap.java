package io.tapdata.entity.schema.type;

import static io.tapdata.entity.simplify.TapSimplify.tapMap;

public class TapMap extends TapType {
    @Override
    public TapType cloneTapType() {
        return tapMap();
    }
}
