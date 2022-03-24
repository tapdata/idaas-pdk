package io.tapdata.entity.mapping.type;

import io.tapdata.entity.type.TapType;
import io.tapdata.entity.utils.DefaultMap;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class TapMapping {
    public static final String FIELD_TYPE_MAPPING = "_tapMapping";

    private String to;
    private Boolean queryOnly;
    private static final Map<String, Class<?>> classCacheMap = new ConcurrentHashMap<>();

    public TapMapping() {}

    protected String getParam(Map<String, String> params, String key) {
        if(params != null)
            return params.get(key);
        return null;
    }

    protected Object getObject(Map<String, Object> map, String key) {
        if(map != null)
            return map.get(key);
        return null;
    }


    /**
     * Handle the value of open type json.
     *
     * for example, the value of below,
     * "decimal($precision, $scale)[unsigned][zerofill]": {"precision":[1, 65], "scale": [0, 30], "unsigned": true, "to": "TapNumber"}
     *
     * @param info
     */
    public abstract void from(Map<String, Object> info);

    public static TapMapping build(DefaultMap info) {
        return TapMapping.build(((Map<String, Object>)info));
    }
    public static TapMapping build(Map<String, Object> info) {
        String to = (String) info.get("to");
        Boolean queryOnly = (Boolean) info.get("queryOnly");
        if(to == null)
            return null;

        String typeMappingClass = TapMapping.class.getPackage().getName() + "." + to + "Mapping";
        Class<?> mappingClass = classCacheMap.get(typeMappingClass);
        if(mappingClass == null) {
            synchronized (classCacheMap) {
                mappingClass = classCacheMap.get(typeMappingClass);
                if(mappingClass == null) {
                    try {
                        mappingClass = Class.forName(typeMappingClass);
                        if(!TapMapping.class.isAssignableFrom(mappingClass)) {
                            return null;
                        }
                        classCacheMap.put(typeMappingClass, mappingClass);
                    } catch (Throwable e) {
                        e.printStackTrace();
                        return null;
                    }
                }
            }
        }

        try {
            TapMapping tapMapping = (TapMapping) mappingClass.getConstructor().newInstance();
            tapMapping.to = to;
            tapMapping.queryOnly = queryOnly;
            tapMapping.from(info);
            return tapMapping;
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return null;
    }

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }

    public abstract TapType toTapType(String originType, Map<String, String> params);

    public abstract String fromTapType(String typeExpression, TapType tapType);

    protected String removeBracketVariables(String typeExpression, int startPos) {
        int pos = typeExpression.indexOf("[", startPos);
        while (pos >= 0) {
            int endPos = typeExpression.indexOf("]", pos);
            int embeddedStartPos = typeExpression.indexOf("[", pos + 1);
            if (embeddedStartPos >= 0 && embeddedStartPos < endPos) {
                typeExpression = removeBracketVariables(typeExpression, embeddedStartPos);
            } else {
                if (endPos >= 0) {
                    typeExpression = typeExpression.substring(0, pos) + typeExpression.substring(endPos + 1);
                } else {
                    typeExpression = typeExpression.substring(0, pos) + typeExpression.substring(pos + 1);
                }
            }
            pos = typeExpression.indexOf("[", startPos);
        }
        return typeExpression;
    }

    protected String clearBrackets(String typeExpression, String str) {
        return clearBrackets(typeExpression, str, true);
    }
    protected String clearBrackets(String typeExpression, String str, boolean needSpace) {
        if(str == null)
            return typeExpression;
        int pos = typeExpression.indexOf(str);
        if (pos >= 0) {
            int startPos = typeExpression.lastIndexOf("[", pos);
            if (startPos >= 0) {
                typeExpression = typeExpression.substring(0, startPos) + (needSpace ? " " : "") + typeExpression.substring(startPos + 1);

                int endPos = typeExpression.indexOf("]", pos);
                if (endPos >= 0) {
                    typeExpression = typeExpression.substring(0, endPos) + typeExpression.substring(endPos + 1);
                }
            }
        }
        return typeExpression;
    }

    public Boolean getQueryOnly() {
        return queryOnly;
    }

    public void setQueryOnly(Boolean queryOnly) {
        this.queryOnly = queryOnly;
    }
}
