package io.tapdata.entity.mapping.type;

import java.util.Map;

public abstract class TapBytesBase extends TapMapping {
    public static final String KEY_FIXED = "fixed";
    public static final String KEY_BYTE = "byte";
    public static final String KEY_BYTE_DEFAULT = "defaultByte";

    protected String fixed;
    protected Long bytes;
    protected Long defaultBytes;

    @Override
    public void from(Map<String, Object> info) {
        Object fixedObj = info.get(KEY_FIXED);
        if(fixedObj instanceof String) {
            fixed = (String) fixedObj;
        }

        Object byteDefaultObject = info.get(KEY_BYTE_DEFAULT);
        defaultBytes = objectToNumber(byteDefaultObject);

        Object byteObj = info.get(KEY_BYTE);
        bytes = objectToNumber(byteObj);
    }

    private Long objectToNumber(Object obj) {
        Long number = null;
        if(obj instanceof Number) {
            number = ((Number) obj).longValue();
        } else if(obj instanceof String) {
            //4g 64k 16m
            String str = (String) obj;
            str = str.trim().toLowerCase();
            if(str.endsWith("k")) {
                number = calculateBytes(str, 1024);
            } else if(str.endsWith("m")) {
                number = calculateBytes(str, 1024L * 1024);
            } else if(str.endsWith("g")) {
                number = calculateBytes(str, 1024L * 1024 * 1024);
            } else if(str.endsWith("t")) {
                number = calculateBytes(str, 1024L * 1024 * 1024 * 1024);
            } else if(str.endsWith("p")) {
                number = calculateBytes(str, 1024L * 1024 * 1024 * 1024 * 1024);
            } else {
                try {
                    number = Long.parseLong(str);
                } catch(Throwable ignored) {}
            }
        }
        return number;
    }

    private Long calculateBytes(String str, long ratio) {
        String numberStr = str.substring(0, str.length() - 1);
        try {
            long num = Long.parseLong(numberStr);
            return num * ratio;
        } catch(Throwable ignored) {}
        return null;
    }

    public String getFixed() {
        return fixed;
    }

    public void setFixed(String fixed) {
        this.fixed = fixed;
    }

    public Long getBytes() {
        return bytes;
    }

    public void setBytes(Long bytes) {
        this.bytes = bytes;
    }
}
