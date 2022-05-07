package io.tapdata.entity.mapping.type;

import io.tapdata.entity.result.ResultItem;
import io.tapdata.entity.result.TapResult;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapBinary;
import io.tapdata.entity.schema.type.TapType;

import java.math.BigDecimal;
import java.util.Map;

/**
 *  "varbinary($width)": {"byte": 255, "fixed": false, "to": "typeBinary"}
 */
public class TapBinaryMapping extends TapBytesBase {

    @Override
    public TapType toTapType(String dataType, Map<String, String> params) {
        return new TapBinary().bytes(getToTapTypeBytes(params));
    }

    @Override
    public TapResult<String> fromTapType(String typeExpression, TapType tapType) {
        String theFinalExpression = null;
        if (tapType instanceof TapBinary) {
            TapResult<String> tapResult = new TapResult<>();
            tapResult.result(TapResult.RESULT_SUCCESSFULLY);
            TapBinary tapBinary = (TapBinary) tapType;
            theFinalExpression = typeExpression;

            Long bytes = tapBinary.getBytes();
            if (bytes != null) {
                bytes = getFromTapTypeBytes(bytes);
                if(this.bytes != null && bytes > this.bytes) {
                    tapResult.addItem(new ResultItem("TapBinaryMapping BYTES", TapResult.RESULT_SUCCESSFULLY_WITH_WARN, "Bytes " + bytes + " from source exceeded the maximum of target bytes " + this.bytes + ", bytes before ratio " + tapBinary.getBytes() + ", expression {}" + typeExpression));
                    bytes = this.bytes;
                    tapResult.result(TapResult.RESULT_SUCCESSFULLY_WITH_WARN);
                }
                theFinalExpression = clearBrackets(theFinalExpression, "$" + KEY_BYTE, false);
                theFinalExpression = theFinalExpression.replace("$" + KEY_BYTE, String.valueOf(bytes));
            }
            theFinalExpression = removeBracketVariables(theFinalExpression, 0);
            return tapResult.data(theFinalExpression);
        }
        return null;
    }

    @Override
    public BigDecimal matchingScore(TapField field) {
        if (field.getTapType() instanceof TapBinary) {
            TapBinary tapBinary = (TapBinary) field.getTapType();

            Long theBytes = bytes;
            if(theBytes != null)
                theBytes = theBytes * byteRatio;
            Long width = tapBinary.getBytes();
            if(width == null && theBytes != null) {
                return BigDecimal.valueOf(theBytes);
            } else if(theBytes != null) {
//                width = getFromTapTypeBytes(width);
                if(width <= theBytes) {
                    return BigDecimal.valueOf(Long.MAX_VALUE - (theBytes - width));
                } else {
                    return BigDecimal.valueOf(theBytes - width); // unacceptable
                }
            }

            return BigDecimal.ZERO;
        }
        return BigDecimal.valueOf(-Double.MAX_VALUE);
    }
}
