package io.tapdata.entity.mapping.type;

import io.tapdata.entity.type.TapMap;
import io.tapdata.entity.type.TapType;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 * {
 *     "tinyint[($m)][unsigned][zerofill]": {"bit": 1, "unsigned": "unsigned", "to": "typeNumber"},
 *
 *     "tinyint[($m)][unsigned][zerofill]": {"bit": 1, "unsigned": "unsigned", "to": "typeNumber"},
 *
 *     "smallint[($m)][unsigned][zerofill]": {"bit": 4, "unsigned": "unsigned", "to": "typeNumber"},
 *     "mediumint[($m)][unsigned][zerofill]": {"bit": 8, "unsigned": "unsigned", "to": "typeNumber"},
 *     "int[($m)][unsigned][zerofill]": {"bit": 32, "unsigned": "unsigned", "to": "typeNumber"},
 *     "bigint[($m)][unsigned][zerofill]": {"bit": 256, "unsigned": "unsigned", "to": "typeNumber"},
 *     "float[($float)][unsigned][zerofill]": {"fbit": 16, "unsigned": "unsigned", "to": "typeNumber"},
 *     "double[($float)][unsigned][zerofill]": {"float": 256, "unsigned": "unsigned", "to": "typeNumber"},
 *     "decimal($precision, $scale)[unsigned][zerofill]": {"precision":[1, 65], "scale": [0, 30], "unsigned": "unsigned", "to": "typeNumber"},
 *     "date": {"range": ["1000-01-01", "9999-12-31"], "to": "typeDate"},
 *     "time": {"range": ["-838:59:59","838:59:59"], "to": "typeInterval:typeNumber"},
 *     "year[($m)]": {"range": [1901, 2155], "to": "typeYear:typeNumber"},
 *     "datetime": {"range": ["1000-01-01 00:00:00", "9999-12-31 23:59:59"], "to": "typeDateTime"},
 *     "timestamp": {"to": "typeDateTime"},
 *     "char[($width)]": {"byte": 255, "to": "typeString"},
 *     "varchar[($width)]": {"byte": "64k", "fixed": false, "to": "typeString"},
 *     "tinyblob": {"byte": 255, "to": "typeBinary"},
 *     "tinytext": {"byte": 255, "to": "typeString"},
 *     "blob": {"byte": "64k", "to": "typeBinary"},
 *     "text": {"byte": "64k", "to": "typeString"},
 *     "mediumblob": {"byte": "16m", "to": "typeBinary"},
 *     "mediumtext": {"byte": "16m", "to": "typeString"},
 *     "longblob": {"byte": "4g", "to": "typeBinary"},
 *     "longtext": {"byte": "4g", "to": "typeString"},
 *     "bit($width)": {"byte": 8, "to": "typeBinary"},
 *     "binary($width)": {"byte": 255, "to": "typeBinary"},
 *     "varbinary($width)": {"byte": 255, "fixed": false, "to": "typeBinary"}
 * }
 */
public abstract class TapMapping {
    private String to;
    private final Type type;

    public TapMapping() {
        Type superClass = getClass().getGenericSuperclass();

        this.type = ((ParameterizedType) superClass).getActualTypeArguments()[0];
    }

    public abstract void from(Map<String, Object> info);

    public static TapMapping build(Map<String, Object> info) {
        String to = (String) info.get("to");
        if(to == null)
            return null;

        String typeMappingClass = TapMapping.class.getPackage().getName() + "." + to;
        try {
            Class<?> mappingClass = Class.forName(typeMappingClass);
            if(!mappingClass.isAssignableFrom(TapMapping.class)) {
                return null;
            }
            TapMapping tapMapping = (TapMapping) mappingClass.getConstructor().newInstance();
            tapMapping.to = to;
            tapMapping.from(info);
            return tapMapping;
        } catch (Throwable e) {
            e.printStackTrace();
            return null;
        }
    }

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }
}
