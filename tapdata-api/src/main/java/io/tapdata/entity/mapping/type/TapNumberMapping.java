package io.tapdata.entity.mapping.type;

import io.tapdata.entity.result.ResultItem;
import io.tapdata.entity.result.TapResult;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.utils.TypeUtils;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.tapNumber;

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
    public static final String KEY_BIT_RATIO = "bitRatio";
    public static final String KEY_VALUE = "value";
    public static final String KEY_UNSIGNED_VALUE = "unsignedValue";
    public static final String KEY_FIXED = "fixed";

    private Boolean fixed;

    private Integer bit;
    private Integer defaultBit;
    private int bitRatio = 1;

    private Integer minPrecision;
    private Integer maxPrecision;
    private Integer defaultPrecision;

    private Integer minScale;
    private Integer maxScale;
    private Integer defaultScale;

    private String unsigned;
    private String zerofill;

    private BigDecimal minValue;
    private BigDecimal maxValue;

    private BigDecimal unsignedMinValue;
    private BigDecimal unsignedMaxValue;

    protected Integer getFromTapTypeBytes(Integer bit) {
        if(bitRatio == 1)
            return bit;
        // 14 / 8 = 2, 14 / 7 = 2
        return (bit / bitRatio + ((bit % bitRatio) > 0 ? 1 : 0));
    }

    @Override
    public void from(Map<String, Object> info) {
        Object fixedObj = getObject(info, KEY_FIXED);
        if (fixedObj instanceof Boolean) {
            fixed = (Boolean) fixedObj;
        }

        Object bitObj = getObject(info, KEY_BIT);
        if (bitObj instanceof Number) {
            bit = ((Number) bitObj).intValue();
        }
        Object defaultLengthObj = getObject(info, KEY_BIT_DEFAULT);
        if(defaultLengthObj instanceof Number) {
            defaultBit = ((Number) defaultLengthObj).intValue();
        }
        Object ratioObj = info.get(KEY_BIT_RATIO);
        if(ratioObj instanceof Number) {
            bitRatio = ((Number) ratioObj).intValue();
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
        } else if(precisionObj instanceof Number) {
            minPrecision = 1;
            maxPrecision = ((Number) precisionObj).intValue();
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
        } else if(scaleObj instanceof Number) {
            minScale = 0;
            maxScale = ((Number) scaleObj).intValue();
        }

        Object defaultScaleObj = getObject(info, KEY_SCALE_DEFAULT);
        if(defaultScaleObj instanceof Number) {
            defaultScale = ((Number) defaultScaleObj).intValue();
        }

        Object valueObj = getObject(info, KEY_VALUE);
        if (valueObj instanceof List) {
            List<?> list = (List<?>) valueObj;
            if (list.size() == 2) {
                if ((list.get(0) instanceof Number) || (list.get(0) instanceof String)) {
                    minValue = new BigDecimal(String.valueOf(list.get(0)));
                }
                if ((list.get(1) instanceof Number) || (list.get(1) instanceof String)) {
                    maxValue = new BigDecimal(String.valueOf(list.get(1)));
                }
            }
        }

        Object unsignedValueObj = getObject(info, KEY_UNSIGNED_VALUE);
        if (unsignedValueObj instanceof List) {
            List<?> list = (List<?>) unsignedValueObj;
            if (list.size() == 2) {
                if ((list.get(0) instanceof Number) || (list.get(0) instanceof String)) {
                    unsignedMinValue = new BigDecimal(String.valueOf(list.get(0)));
                }
                if ((list.get(1) instanceof Number) || (list.get(1) instanceof String)) {
                    unsignedMaxValue = new BigDecimal(String.valueOf(list.get(1)));
                }
            }
        }

        Object unsignedObj = getObject(info, KEY_UNSIGNED);
        if (unsignedObj instanceof String) {
            unsigned = (String) unsignedObj;
        }

        Object zerofillObj = getObject(info, KEY_ZEROFILL);
        if (zerofillObj instanceof String) {
            zerofill = (String) zerofillObj;
        }

        //calculate the min and max value.
        //The accuracy order, min/max value > bit > precision.
        if(minValue == null || maxValue == null) {
            //bit is higher priority than precision, lower than given min/max value.
            Integer theBit = bit;
            if(theBit == null)
                theBit = defaultBit;
            if(theBit != null) {
                if(unsigned != null && unsignedMinValue == null) {
                    unsignedMinValue = TypeUtils.minValueForBit(theBit, true);
                }
                if(minValue == null)
                    minValue = TypeUtils.minValueForBit(theBit, false);
                if(unsigned != null && unsignedMaxValue == null) {
                    unsignedMaxValue = TypeUtils.maxValueForBit(theBit, true);
                }
                if(maxValue == null)
                    maxValue = TypeUtils.maxValueForBit(theBit, false);
            }
        }
        if(minValue == null || maxValue == null) {
            //precision is lowest priority, for the value boundary, the most case, the precision is not accurate.
            Integer thePrecision = maxPrecision;
            if(thePrecision == null)
                thePrecision = defaultPrecision;
            if(thePrecision != null) {
                if(unsigned != null && unsignedMinValue == null) {
                    unsignedMinValue = BigDecimal.ZERO;
                }
                if(minValue == null)
                    minValue = TypeUtils.minValueForPrecision(thePrecision);
                if(unsigned != null && unsignedMaxValue == null) {
                    unsignedMaxValue = TypeUtils.maxValueForPrecision(thePrecision);
                }
                if(maxValue == null)
                    maxValue = TypeUtils.maxValueForPrecision(thePrecision);
            }
        }
        if(minValue == null || maxValue == null) {
            minValue = BigDecimal.valueOf(-Double.MAX_VALUE);
            maxValue = BigDecimal.valueOf(Double.MAX_VALUE);
        }
        if(minPrecision == null && maxPrecision == null) {
            minPrecision = 1;
            maxPrecision = maxValue.precision();
        }
    }

    @Override
    public TapType toTapType(String dataType, Map<String, String> params) {
        Boolean theUnsigned = null;
        if (unsigned != null && dataType.contains(unsigned)) {
            theUnsigned = true;
        }
        Boolean theZerofill = null;
        if (zerofill != null && dataType.contains(zerofill)) {
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
        if(length == null)
            length = bit;
        if(length != null)
            length = length * bitRatio;

        String precisionStr = getParam(params, KEY_PRECISION);
        Integer precision = null;
        if (precisionStr != null) {
            precisionStr = precisionStr.trim();
            try {
                precision = Integer.parseInt(precisionStr);
            } catch (Throwable throwable) {
                throwable.printStackTrace();
            }
        }
        if(precision == null)
            precision = defaultPrecision;
        if(precision == null)
            precision = maxPrecision;

        String scaleStr = getParam(params, KEY_SCALE);
        Integer scale = null;
        if (scaleStr != null) {
            scaleStr = scaleStr.trim();
            try {
                scale = Integer.parseInt(scaleStr);
            } catch (Throwable throwable) {
                throwable.printStackTrace();
            }
        }
        if(scale == null)
            scale = defaultScale;
        if(scale == null)
            scale = maxScale;

        BigDecimal theMinValue = null;
        BigDecimal theMaxValue = null;
        if(length != null) {
            theMinValue = TypeUtils.minValueForBit(length, theUnsigned);
            theMaxValue = TypeUtils.maxValueForBit(length, theUnsigned);
        } else if(precision != null){
            theMinValue = (theUnsigned != null && theUnsigned) ? BigDecimal.ZERO : TypeUtils.minValueForPrecision(precision);
            theMaxValue = TypeUtils.maxValueForPrecision(precision);
        } else {
            theMinValue = BigDecimal.valueOf(-Double.MAX_VALUE);
            theMaxValue = BigDecimal.valueOf(Double.MAX_VALUE);
        }

        return tapNumber()
                .fixed(fixed)
                .precision(precision)
                .scale(scale)
                .bit(length)
                .minValue(theMinValue)
                .maxValue(theMaxValue)
                .unsigned(theUnsigned)
                .zerofill(theZerofill);
    }
    final BigDecimal valueValue = BigDecimal.valueOf(10000000000000000000000000000000000000000000000000000000000000000000000000000000000000d);
    final BigDecimal scaleValue = BigDecimal.valueOf(1000000000000000000000000000000000000000000000000000000000000000000000000000000000000d);
    final BigDecimal fixedValue = BigDecimal.valueOf(100000000000000000000000000000000000000000000000000000000000000000000000000000000000d);
    final BigDecimal unsignedValue = BigDecimal.valueOf(10000000000000000000000000000000000000000000000000000000000000000000000000000000000d);
    @Override
    public BigDecimal matchingScore(TapField field) {
        if (field.getTapType() instanceof TapNumber) {
            TapNumber tapNumber = (TapNumber) field.getTapType();

            BigDecimal score = BigDecimal.ZERO;

            Integer scale = tapNumber.getScale();
            Boolean fixed = tapNumber.getFixed();
            Boolean unsigned = tapNumber.getUnsigned();

            BigDecimal comingMaxValue = tapNumber.getMaxValue();
            BigDecimal comingMinValue = tapNumber.getMinValue();

            if((scale != null && isScale()) ||
                    (scale == null && !isScale())) {
//                score += scaleValue;
                score = score.add(scaleValue);
            } else {
//                score -= scaleValue;
                score = score.subtract(scaleValue);
            }

            if(((fixed != null && fixed) && (this.fixed != null && this.fixed)) ||
                    ((fixed == null || !fixed) && (this.fixed == null || !this.fixed))) {
//                score += fixedValue;
                score = score.add(fixedValue);
            } else {
//                score -= fixedValue;
                score = score.subtract(fixedValue);
            }

            if(((unsigned != null && unsigned) && (this.unsigned != null)) ||
                    ((unsigned == null || !unsigned) && (this.unsigned == null))) {
//                score += unsignedValue;
                score = score.add(unsignedValue);
            } else {
//                score -= unsignedValue;
                score = score.subtract(unsignedValue);
            }

            if(unsigned != null && unsigned) {
                //unsigned number
                if(unsignedMinValue != null && unsignedMaxValue != null) {
                    score = score.add(calculateScoreForValue(comingMinValue, comingMaxValue, unsignedMinValue, unsignedMaxValue));
                } else {
                    score = score.add(calculateScoreForValue(comingMinValue, comingMaxValue, minValue, maxValue));
                }
            } else {
                //singed number
                score = score.add(calculateScoreForValue(comingMinValue, comingMaxValue, minValue, maxValue));
            }

//            Integer precision = tapNumber.getPrecision();
//            if(precision != null && scale != null && minPrecision != null && minScale != null && maxPrecision != null && maxScale != null) {
//                if(minPrecision <= precision && precision <= maxPrecision) {
//                    score += 1000L - (maxPrecision - precision); // The closest to maxPrecision the better.
//                } else {
//                    if(precision > maxPrecision) {
//                        return (maxPrecision - precision);
//                    }
//                    return Long.MIN_VALUE; //if precision didn't match, it is not acceptable
//                }
//                if(minScale <= scale && scale <= maxScale) {
//                    score += 500;
//                } else {
//                    score += 1; //loss scale, somehow is acceptable as lowest priority
//                }
//            }
//
//            Integer bit = tapNumber.getBit();
//            if(bit != null && this.bit != null) {
//                int theBit = this.bit * bitRatio;
//                if(0 < bit && bit <= theBit) {
//                    score = 1000L - (theBit - bit); //The closest to max bit, the better
//                } else {
////                    if(bit > theBit) {
////                        return theBit - bit;
////                    }
//                    return Long.MIN_VALUE; //if bit didn't match, it is not acceptable
//                }
//            }
//
//            if(tapNumber.getUnsigned() != null && tapNumber.getUnsigned() && unsigned != null) {
//                //number is unsigned, current mapping support unsigned, closer.
//                score += 10;
//            }
//
//            if(tapNumber.getZerofill() != null && tapNumber.getZerofill() && zerofill != null) {
//                //number is zerofill, current mapping support zerofill, closer.
//                score += 1;
//            }
            return score;
        }

        return BigDecimal.valueOf(-Double.MAX_VALUE);
    }

    private BigDecimal calculateScoreForValue(BigDecimal comingMinValue, BigDecimal comingMaxValue, BigDecimal minValue, BigDecimal maxValue) {
        BigDecimal minDistance = comingMinValue.subtract(minValue);
        BigDecimal maxDistance = maxValue.subtract(comingMaxValue);

        if (minDistance.compareTo(BigDecimal.ZERO) < 0 || maxDistance.compareTo(BigDecimal.ZERO) < 0) {
            BigDecimal theDistance = minDistance.add(maxDistance).abs();
            if(theDistance.compareTo(valueValue) > 0) {
                return valueValue.add(valueValue).negate();//-valueValue - valueValue;
            } else {
                return valueValue.add(theDistance).negate();//-valueValue - theDistance.negate().doubleValue();
            }
        } else {
            BigDecimal valueDistance = valueValue.subtract(minDistance.add(maxDistance));
            if(valueDistance.compareTo(BigDecimal.ZERO) < 0) {
                return BigDecimal.ZERO;
            }
            return valueDistance;
        }
    }

    private boolean isScale() {
        return this.defaultScale != null || (this.minScale != null && this.maxScale != null);
    }

    @Override
    public TapResult<String> fromTapType(String typeExpression, TapType tapType) {
        String theFinalExpression = null;
        TapResult<String> tapResult = new TapResult<>();
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
                int bit = getFromTapTypeBytes(tapNumber.getBit());
                if(this.bit != null && bit > this.bit) {
                    tapResult.addItem(new ResultItem("TapNumberMapping BIT", TapResult.RESULT_SUCCESSFULLY_WITH_WARN, "Bit " + bit + " from source exceeded the maximum of target bit " + this.bit + ", bit before ratio " + tapNumber.getBit() + ", expression " + typeExpression));
                    bit = this.bit;
                }
                theFinalExpression = theFinalExpression.replace("$" + KEY_BIT, String.valueOf(bit));
            }
            Integer precision = tapNumber.getPrecision();
            if (precision != null) {
                theFinalExpression = clearBrackets(theFinalExpression, "$" + KEY_PRECISION, false);

                if(this.maxPrecision != null && this.minPrecision != null) {
                    if(minPrecision > precision) {
                        tapResult.addItem(new ResultItem("TapNumberMapping MIN_PRECISION", TapResult.RESULT_SUCCESSFULLY_WITH_WARN, "Precision " + precision + " from source exceeded the minimum of target precision " + this.minPrecision + ", expression " + typeExpression));
                        precision = minPrecision;
                    } else if(maxPrecision < precision) {
                        tapResult.addItem(new ResultItem("TapNumberMapping MAX_PRECISION", TapResult.RESULT_SUCCESSFULLY_WITH_WARN, "Precision " + precision + " from source exceeded the maximum of target precision " + this.maxPrecision + ", expression " + typeExpression));
                        precision = maxPrecision;
                    }
                }
                theFinalExpression = theFinalExpression.replace("$" + KEY_PRECISION, String.valueOf(precision));
            }
            Integer scale = tapNumber.getScale();
            if (scale != null) {
                theFinalExpression = clearBrackets(theFinalExpression, "$" + KEY_SCALE, false);

                if(minScale != null && maxScale != null) {
                    if(minScale > scale) {
                        tapResult.addItem(new ResultItem("TapNumberMapping MIN_SCALE", TapResult.RESULT_SUCCESSFULLY_WITH_WARN, "Scale " + scale + " from source exceeded the minimum of target scale " + this.minScale + ", expression " + typeExpression));
                        scale = minScale;
                    } else if(maxScale < scale) {
                        tapResult.addItem(new ResultItem("TapNumberMapping MAX_SCALE", TapResult.RESULT_SUCCESSFULLY_WITH_WARN, "Scale " + scale + " from source exceeded the maxiumu of target scale " + this.maxScale + ", expression " + typeExpression));
                        scale = maxScale;
                    }
                }
                theFinalExpression = theFinalExpression.replace("$" + KEY_SCALE, String.valueOf(scale));
            }
            theFinalExpression = removeBracketVariables(theFinalExpression, 0);
        }
        if(tapResult.getResultItems() != null && !tapResult.getResultItems().isEmpty())
            tapResult.result(TapResult.RESULT_SUCCESSFULLY_WITH_WARN);
        else
            tapResult.result(TapResult.RESULT_SUCCESSFULLY);
        return tapResult.data(theFinalExpression);
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

    public Integer getDefaultBit() {
        return defaultBit;
    }

    public void setDefaultBit(Integer defaultBit) {
        this.defaultBit = defaultBit;
    }

    public Integer getDefaultPrecision() {
        return defaultPrecision;
    }

    public void setDefaultPrecision(Integer defaultPrecision) {
        this.defaultPrecision = defaultPrecision;
    }

    public Integer getDefaultScale() {
        return defaultScale;
    }

    public void setDefaultScale(Integer defaultScale) {
        this.defaultScale = defaultScale;
    }
}
