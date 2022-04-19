package io.tapdata.entity.schema.type;

import static io.tapdata.entity.simplify.TapSimplify.tapDate;

public class TapDate extends TapType {
    @Override
    public TapType cloneTapType() {
        return tapDate();
    }
}
