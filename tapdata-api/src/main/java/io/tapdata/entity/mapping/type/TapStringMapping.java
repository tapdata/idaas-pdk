package io.tapdata.entity.mapping.type;

import io.tapdata.entity.result.ResultItem;
import io.tapdata.entity.result.TapResult;
import io.tapdata.entity.schema.TapField;
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
        return new TapString().bytes(getToTapTypeBytes(params)).fixed(theFixed);
    }

    @Override
    public TapResult<String> fromTapType(String typeExpression, TapType tapType) {
        String theFinalExpression = null;
        if (tapType instanceof TapString) {
            TapResult<String> tapResult = new TapResult<>();
            tapResult.result(TapResult.RESULT_SUCCESSFULLY);
            TapString tapString = (TapString) tapType;
            theFinalExpression = typeExpression;
            if (tapString.getFixed() != null && tapString.getFixed()) {
                theFinalExpression = clearBrackets(theFinalExpression, fixed);
            }

            Long bytes = tapString.getBytes();
            if (bytes != null) {
                bytes = getFromTapTypeBytes(bytes);
                if(this.bytes != null && bytes > this.bytes) {
                    tapResult.addItem(new ResultItem("TapStringMapping BYTES", TapResult.RESULT_SUCCESSFULLY_WITH_WARN, "Bytes " + bytes + " from source exceeded the maximum of target bytes " + this.bytes + ", bytes before ratio " + tapString.getBytes() + ", expression {}" + typeExpression));
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
    public long matchingScore(TapField field) {
        if (field.getTapType() instanceof TapString) {
            TapString tapString = (TapString) field.getTapType();

            Long theBytes = bytes;
            if(theBytes != null)
                theBytes = theBytes * byteRatio;
            Long width = tapString.getBytes();
            if(width == null && theBytes != null) {
                return theBytes;
            } else if(theBytes != null) {
                width = getFromTapTypeBytes(width);
                if(width <= theBytes) {
                    return (Long.MAX_VALUE - (theBytes - width));
                } else {
                    return -1L; // unacceptable
                }
            }

            return 0L;
        }
        return -1L;
    }
}
