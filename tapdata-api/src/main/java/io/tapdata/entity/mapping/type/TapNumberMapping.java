package io.tapdata.entity.mapping.type;

import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.type.TapType;

import java.util.List;
import java.util.Map;

/**
 * "decimal($precision, $scale)[unsigned][zerofill]": {"precision":[1, 65], "scale": [0, 30], "unsigned": true, "to": "typeNumber"},
 * "int[($length)][unsigned][zerofill]": {"length": 32, "unsigned": true, "to": "TapNumber"},
 */
public class TapNumberMapping extends TapMapping {
    public static final String KEY_PRECISION = "precision";
    public static final String KEY_PRECISION_DEFAULT = "precisionDefault";
    public static final String KEY_SCALE = "scale";
    public static final String KEY_SCALE_DEFAULT = "scaleDefault";
    public static final String KEY_UNSIGNED = "unsigned";
    public static final String KEY_ZEROFILL = "zerofill";
    public static final String KEY_BIT = "bit";
    public static final String KEY_BIT_DEFAULT = "bitDefault";

    private Integer bit;
    private Integer defaultBit;

    private Integer minPrecision;
    private Integer maxPrecision;
    private Integer defaultPrecision;

    private Integer minScale;
    private Integer maxScale;
    private Integer defaultScale;

    private String unsigned;
    private String zerofill;


    @Override
    public void from(Map<String, Object> info) {
        Object bitObj = getObject(info, KEY_BIT);
        if (bitObj instanceof Number) {
            bit = ((Number) bitObj).intValue();
        }
        Object defaultLengthObj = getObject(info, KEY_BIT_DEFAULT);
        if(defaultLengthObj instanceof Number) {
            defaultBit = ((Number) defaultLengthObj).intValue();
        }

        Object precisionObj = getObject(info, KEY_PRECISION);
        if (precisionObj instanceof List) {
            List<?> list = (List<?>) precisionObj;
            if (list.size() == 2) {
                if (list.get(0) instanceof Number) {
                    minPrecision = ((Number) list.get(0)).intValue();
                }
                if (list.get(1) instanceof Number) {
                    maxPrecision = ((Number) list.get(1)).intValue();
                }
            }
        }
        Object defaultPrecisionObj = getObject(info, KEY_PRECISION_DEFAULT);
        if(defaultPrecisionObj instanceof Number) {
            defaultPrecision = ((Number) defaultPrecisionObj).intValue();
        }

        Object scaleObj = getObject(info, KEY_SCALE);
        if (scaleObj instanceof List) {
            List<?> list = (List<?>) scaleObj;
            if (list.size() == 2) {
                if (list.get(0) instanceof Number) {
                    minScale = ((Number) list.get(0)).intValue();
                }
                if (list.get(1) instanceof Number) {
                    maxScale = ((Number) list.get(1)).intValue();
                }
            }
        }

        Object defaultScaleObj = getObject(info, KEY_SCALE_DEFAULT);
        if(defaultScaleObj instanceof Number) {
            defaultScale = ((Number) defaultScaleObj).intValue();
        }

        Object unsignedObj = getObject(info, KEY_UNSIGNED);
        if (unsignedObj instanceof String) {
            unsigned = (String) unsignedObj;
        }

        Object zerofillObj = getObject(info, KEY_ZEROFILL);
        if (zerofillObj instanceof String) {
            zerofill = (String) zerofillObj;
        }
    }

    @Override
    public TapType toTapType(String originType, Map<String, String> params) {
        Boolean theUnsigned = null;
        if (unsigned != null && originType.contains(unsigned)) {
            theUnsigned = true;
        }
        Boolean theZerofill = null;
        if (zerofill != null && originType.contains(zerofill)) {
            theZerofill = true;
        }

        String lengthStr = getParam(params, KEY_BIT);
        Integer length = null;
        if (lengthStr != null) {
            try {
                length = Integer.parseInt(lengthStr);
            } catch (Throwable throwable) {
                throwable.printStackTrace();
            }
        }
        if(length == null)
            length = defaultBit;

        String precisionStr = getParam(params, KEY_PRECISION);
        Integer precision = null;
        if (precisionStr != null) {
            try {
                precision = Integer.parseInt(precisionStr);
            } catch (Throwable throwable) {
                throwable.printStackTrace();
            }
        }
        if(precision == null)
            precision = defaultPrecision;

        String scaleStr = getParam(params, KEY_SCALE);
        Integer scale = null;
        if (scaleStr != null) {
            try {
                scale = Integer.parseInt(scaleStr);
            } catch (Throwable throwable) {
                throwable.printStackTrace();
            }
        }
        if(scale == null)
            scale = defaultScale;

        return new TapNumber()
                .precision(precision)
                .scale(scale)
                .bit(length)
                .unsigned(theUnsigned)
                .zerofill(theZerofill);
    }

    @Override
    public String fromTapType(String typeExpression, TapType tapType) {
        String theFinalExpression = null;
        if (tapType instanceof TapNumber) {
            TapNumber tapNumber = (TapNumber) tapType;
            theFinalExpression = typeExpression;
            if (tapNumber.getUnsigned() != null && tapNumber.getUnsigned()) {
                theFinalExpression = clearBrackets(theFinalExpression, unsigned);
            }
            if (tapNumber.getZerofill() != null && tapNumber.getZerofill()) {
                theFinalExpression = clearBrackets(theFinalExpression, zerofill);
            }

            if (tapNumber.getBit() != null) {
                theFinalExpression = clearBrackets(theFinalExpression, "$" + KEY_BIT, false);
                theFinalExpression = theFinalExpression.replace("$" + KEY_BIT, String.valueOf(tapNumber.getBit()));
            }
            if (tapNumber.getPrecision() != null) {
                theFinalExpression = clearBrackets(theFinalExpression, "$" + KEY_PRECISION, false);
                theFinalExpression = theFinalExpression.replace("$" + KEY_PRECISION, String.valueOf(tapNumber.getPrecision()));
            }
            if (tapNumber.getScale() != null) {
                theFinalExpression = clearBrackets(theFinalExpression, "$" + KEY_SCALE, false);
                theFinalExpression = theFinalExpression.replace("$" + KEY_SCALE, String.valueOf(tapNumber.getScale()));
            }
            theFinalExpression = removeBracketVariables(theFinalExpression, 0);
        }
        return theFinalExpression;
    }

    public Integer getMinPrecision() {
        return minPrecision;
    }

    public void setMinPrecision(Integer minPrecision) {
        this.minPrecision = minPrecision;
    }

    public Integer getMaxPrecision() {
        return maxPrecision;
    }

    public void setMaxPrecision(Integer maxPrecision) {
        this.maxPrecision = maxPrecision;
    }

    public Integer getMinScale() {
        return minScale;
    }

    public void setMinScale(Integer minScale) {
        this.minScale = minScale;
    }

    public Integer getMaxScale() {
        return maxScale;
    }

    public void setMaxScale(Integer maxScale) {
        this.maxScale = maxScale;
    }

    public String getUnsigned() {
        return unsigned;
    }

    public void setUnsigned(String unsigned) {
        this.unsigned = unsigned;
    }

    public String getZerofill() {
        return zerofill;
    }

    public void setZerofill(String zerofill) {
        this.zerofill = zerofill;
    }

    public Integer getBit() {
        return bit;
    }

    public void setBit(Integer bit) {
        this.bit = bit;
    }
}
