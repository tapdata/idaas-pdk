package io.tapdata.entity.mapping.type;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static io.tapdata.entity.utils.TypeUtils.objectToNumber;

/**
 * "date": {"range": ["1000-01-01", "9999-12-31"], "gmt" : 0, "to": "typeDate"},
 */
public abstract class TapDateBase extends TapMapping {
    public static final String KEY_RANGE = "range";
    public static final String KEY_WITH_TIMEZONE = "withTimezone";
    public static final String KEY_BYTE = "byte";
    public static final String KEY_FRACTION = "fraction";
    public static final String KEY_PATTERN = "pattern";
    public static final String KEY_DEFAULT_FRACTION = "defaultFraction";

    private Long bytes;
    private Date min;
    private Date max;
    private String pattern;
    private Integer minFraction;
    private Integer maxFraction;
    private Integer defaultFraction;

    private Boolean withTimeZone;

    protected abstract String pattern();

    @Override
    public void from(Map<String, Object> info) {
        Object patternObj = info.get(KEY_PATTERN);
        if(patternObj instanceof String) {
            pattern = (String) patternObj;
        }

        Object rangeObj = info.get(KEY_RANGE);
        if(rangeObj instanceof List) {
            List<?> list = (List<?>) rangeObj;
            if(list.size() == 2) {
                String thePattern = this.pattern != null ? this.pattern : pattern();
                if(thePattern != null) {
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(thePattern);

                    String minStr;
                    String maxStr;
                    if(list.get(0) instanceof String) {
                        minStr = (String) list.get(0);
                        try {
                            min = simpleDateFormat.parse(minStr);
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                    }
                    if(list.get(1) instanceof String) {
                        maxStr = (String) list.get(1);
                        try {
                            max = simpleDateFormat.parse(maxStr);
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                    }
                    //both must be not null
                    if(min == null || max == null) {
                        min = null;
                        max = null;
                    }
                }
            }
        }

        Object precisionObj = getObject(info, KEY_FRACTION);
        if (precisionObj instanceof List) {
            List<?> list = (List<?>) precisionObj;
            if (list.size() == 2) {
                if (list.get(0) instanceof Number) {
                    minFraction = ((Number) list.get(0)).intValue();
                }
                if (list.get(1) instanceof Number) {
                    maxFraction = ((Number) list.get(1)).intValue();
                }
            }
        } else if(precisionObj instanceof Number) {
            minFraction = 0;
            maxFraction = ((Number) precisionObj).intValue();
        }
        Object defaultPrecisionObj = getObject(info, KEY_DEFAULT_FRACTION);
        if(defaultPrecisionObj instanceof Number) {
            defaultFraction = ((Number) defaultPrecisionObj).intValue();
        }

        Object withTimeZoneObj = info.get(KEY_WITH_TIMEZONE);
        if(withTimeZoneObj instanceof Boolean) {
            withTimeZone = (Boolean) withTimeZoneObj;
        }

        Object byteObj = info.get(KEY_BYTE);
        if(byteObj instanceof Number) {
            bytes = objectToNumber(byteObj);
        }
    }

    public Date getMin() {
        return min;
    }

    public void setMin(Date min) {
        this.min = min;
    }

    public Date getMax() {
        return max;
    }

    public void setMax(Date max) {
        this.max = max;
    }

    public Long getBytes() {
        return bytes;
    }

    public void setBytes(Long bytes) {
        this.bytes = bytes;
    }

    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    public Integer getMinFraction() {
        return minFraction;
    }

    public void setMinFraction(Integer minFraction) {
        this.minFraction = minFraction;
    }

    public Integer getMaxFraction() {
        return maxFraction;
    }

    public void setMaxFraction(Integer maxFraction) {
        this.maxFraction = maxFraction;
    }

    public Integer getDefaultFraction() {
        return defaultFraction;
    }

    public void setDefaultFraction(Integer defaultFraction) {
        this.defaultFraction = defaultFraction;
    }

    public Boolean getWithTimeZone() {
        return withTimeZone;
    }

    public void setWithTimeZone(Boolean withTimeZone) {
        this.withTimeZone = withTimeZone;
    }
}
