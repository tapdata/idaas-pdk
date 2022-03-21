package io.tapdata.entity.mapping.type;

import java.util.List;
import java.util.Map;

/**
 */
public class TapYearMapping extends TapMapping {
    public static final String KEY_RANGE = "range";

    private Integer minRange;
    private Integer maxRange;

    @Override
    public void from(Map<String, Object> info) {
        Object precisionObj = info.get(KEY_RANGE);
        if(precisionObj instanceof List) {
            List<?> list = (List<?>) precisionObj;
            if(list.size() == 2) {
                if(list.get(0) instanceof Number) {
                    minRange = (Integer) list.get(0);
                }
                if(list.get(1) instanceof Number) {
                    maxRange = (Integer) list.get(1);
                }
            }
        }
    }

    public Integer getMinRange() {
        return minRange;
    }

    public void setMinRange(Integer minRange) {
        this.minRange = minRange;
    }

    public Integer getMaxRange() {
        return maxRange;
    }

    public void setMaxRange(Integer maxRange) {
        this.maxRange = maxRange;
    }
}
