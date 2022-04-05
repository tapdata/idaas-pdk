package io.tapdata.entity.mapping.type;

import io.tapdata.entity.schema.type.TapBinary;
import io.tapdata.entity.schema.type.TapType;

import java.util.Map;

/**
 *  "varbinary($width)": {"byte": 255, "fixed": false, "to": "typeBinary"}
 */
public class TapBinaryMapping extends TapBytesBase {

    @Override
    public TapType toTapType(String originType, Map<String, String> params) {
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
        return new TapBinary().width(bytes);
    }

    @Override
    public String fromTapType(String typeExpression, TapType tapType) {
        String theFinalExpression = null;
        if (tapType instanceof TapBinary) {
            TapBinary tapBinary = (TapBinary) tapType;
            theFinalExpression = typeExpression;

            if (tapBinary.getWidth() != null) {
                theFinalExpression = clearBrackets(theFinalExpression, "$" + KEY_BYTE, false);
                theFinalExpression = theFinalExpression.replace("$" + KEY_BYTE, String.valueOf(tapBinary.getWidth()));
            }
            theFinalExpression = removeBracketVariables(theFinalExpression, 0);
        }
        return theFinalExpression;
    }
}
