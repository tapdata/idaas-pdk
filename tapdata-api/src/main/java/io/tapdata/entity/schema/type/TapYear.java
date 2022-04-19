package io.tapdata.entity.schema.type;

import static io.tapdata.entity.simplify.TapSimplify.tapYear;

public class TapYear extends TapType {

    @Override
    public TapType cloneTapType() {
        return tapYear();
    }
}
