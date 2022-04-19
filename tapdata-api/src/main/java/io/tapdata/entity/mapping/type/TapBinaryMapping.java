package io.tapdata.entity.mapping.type;

import io.tapdata.entity.schema.TapField;
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
        if(bytes == null)
            bytes = this.bytes;
        return new TapBinary().bytes(bytes);
    }

    @Override
    public String fromTapType(String typeExpression, TapType tapType) {
        String theFinalExpression = null;
        if (tapType instanceof TapBinary) {
            TapBinary tapBinary = (TapBinary) tapType;
            theFinalExpression = typeExpression;

            if (tapBinary.getBytes() != null) {
                theFinalExpression = clearBrackets(theFinalExpression, "$" + KEY_BYTE, false);
                theFinalExpression = theFinalExpression.replace("$" + KEY_BYTE, String.valueOf(tapBinary.getBytes()));
            }
            theFinalExpression = removeBracketVariables(theFinalExpression, 0);
        }
        return theFinalExpression;
    }

    @Override
    public long matchingScore(TapField field) {
        if (field.getTapType() instanceof TapBinary) {
            TapBinary tapBinary = (TapBinary) field.getTapType();

            Long width = tapBinary.getBytes();
            if(width == null && bytes != null) {
                return bytes;
            } else if(bytes != null) {
                if(width <= bytes) {
                    return (Long.MAX_VALUE - (bytes - width));
                } else {
                    return -1L; // unacceptable
                }
            }

            return 0L;
        }
        return -1L;
    }
}
