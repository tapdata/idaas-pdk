package io.tapdata.entity.mapping.type;

import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.schema.type.TapType;

import java.util.Map;

public class TapStringMapping extends TapBytesBase {

    @Override
    public TapType toTapType(String originType, Map<String, String> params) {
        Boolean theFixed = null;
        if (fixed != null && originType.contains(fixed)) {
            theFixed = true;
        }

        String byteStr = getParam(params, KEY_BYTE);
        Long bytes = null;
        if(byteStr != null) {
            try {
                bytes = Long.parseLong(byteStr);
            } catch(Throwable throwable) {
                throwable.printStackTrace();
            }
        }
        if(bytes == null)
            bytes = defaultBytes;
        return new TapString().width(bytes).fixed(theFixed);
    }

    @Override
    public String fromTapType(String typeExpression, TapType tapType) {
        String theFinalExpression = null;
        if (tapType instanceof TapString) {
            TapString tapString = (TapString) tapType;
            theFinalExpression = typeExpression;
            if (tapString.getFixed() != null && tapString.getFixed()) {
                theFinalExpression = clearBrackets(theFinalExpression, fixed);
            }

            if (tapString.getWidth() != null) {
                theFinalExpression = clearBrackets(theFinalExpression, "$" + KEY_BYTE, false);
                theFinalExpression = theFinalExpression.replace("$" + KEY_BYTE, String.valueOf(tapString.getWidth()));
            }
            theFinalExpression = removeBracketVariables(theFinalExpression, 0);
        }
        return theFinalExpression;
    }
}
