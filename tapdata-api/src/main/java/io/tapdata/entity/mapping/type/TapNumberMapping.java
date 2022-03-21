package io.tapdata.entity.mapping.type;

import java.util.List;
import java.util.Map;

/**
 *  "decimal($precision, $scale)[unsigned][zerofill]": {"precision":[1, 65], "scale": [0, 30], "unsigned": "unsigned", "to": "typeNumber"},
 */
public class TapNumberMapping extends TapMapping {
    public static final String KEY_PRECISION = "precision";
    public static final String KEY_SCALE = "scale";
    public static final String KEY_UNSIGNED = "unsigned";
    public static final String KEY_BIT = "bit";

    private Integer bit;

    private Integer minPrecision;
    private Integer maxPrecision;

    private Integer minScale;
    private Integer maxScale;

    private Boolean unsigned;


    @Override
    public void from(Map<String, Object> info) {
        Object bitObj = info.get(KEY_BIT);
        if(bitObj instanceof Number) {
            bit = (Integer) bitObj;
        }
        Object precisionObj = info.get(KEY_PRECISION);
        if(precisionObj instanceof List) {
            List<?> list = (List<?>) precisionObj;
            if(list.size() == 2) {
                if(list.get(0) instanceof Number) {
                    minPrecision = (Integer) list.get(0);
                }
                if(list.get(1) instanceof Number) {
                    maxPrecision = (Integer) list.get(1);
                }
            }
        }

        Object scaleObj = info.get(KEY_SCALE);
        if(scaleObj instanceof List) {
            List<?> list = (List<?>) scaleObj;
            if(list.size() == 2) {
                if(list.get(0) instanceof Number) {
                    minScale = (Integer) list.get(0);
                }
                if(list.get(1) instanceof Number) {
                    maxScale = (Integer) list.get(1);
                }
            }
        }

        Object unsignedObj = info.get(KEY_UNSIGNED);
        if(unsignedObj instanceof Boolean) {
            unsigned = (Boolean) unsignedObj;
        }
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

    public Boolean getUnsigned() {
        return unsigned;
    }

    public void setUnsigned(Boolean unsigned) {
        this.unsigned = unsigned;
    }

    public Integer getBit() {
        return bit;
    }

    public void setBit(Integer bit) {
        this.bit = bit;
    }
}
