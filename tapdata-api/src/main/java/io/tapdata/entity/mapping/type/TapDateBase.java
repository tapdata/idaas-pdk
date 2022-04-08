package io.tapdata.entity.mapping.type;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * "date": {"range": ["1000-01-01", "9999-12-31"], "gmt" : 0, "to": "typeDate"},
 */
public abstract class TapDateBase extends TapMapping {
    public static final String KEY_RANGE = "range";
    public static final String KEY_GMT = "gmt";
    public static final String KEY_BYTE = "byte";

    private Long bytes;
    private Date min;
    private Date max;

    private Integer gmt;

    protected abstract String pattern();

    @Override
    public void from(Map<String, Object> info) {
        Object rangeObj = info.get(KEY_RANGE);
        if(rangeObj instanceof List) {
            List<?> list = (List<?>) rangeObj;
            if(list.size() == 2) {
                String pattern = pattern();
                if(pattern != null) {
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);

                    String minStr, maxStr;
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

        Object gmtObj = info.get(KEY_GMT);
        if(gmtObj instanceof Number) {
            gmt = ((Number) gmtObj).intValue();
        } else {
            gmt = 0;
        }

        Object byteObj = info.get(KEY_BYTE);
        if(byteObj instanceof Number) {
            bytes = ((Number) byteObj).longValue();
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

    public Integer getGmt() {
        return gmt;
    }

    public void setGmt(Integer gmt) {
        this.gmt = gmt;
    }
}
