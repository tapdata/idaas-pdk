package io.tapdata.entity.mapping;

import io.tapdata.entity.utils.DefaultMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.entity.utils.TypeHolder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Analyze open type json, like below,
 * {
 *     "tinyint[($m)][unsigned][zerofill]": {"bit": 1, "unsigned": "unsigned", "to": "typeNumber"},
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
 *
 * For example,
 *  //longtext
 *  expressionMatchingMap.get("longtext") == {"byte": "4g", "to": "typeString"}
 *  //bit($width)
 *  expressionMatchingMap.get("bit(4)") == {"byte": "4g", "to": "typeString", "_params" : {"width" : "4"}}
 *  //decimal($precision, $scale)[unsigned][zerofill]
 *  expressionMatchingMap.get("decimal(5, 2) unsigned") == {"precision":[1, 65], "scale": [0, 30], "unsigned": "unsigned", "to": "typeNumber", "_params" : {"precision" : "5", "scale" : "2", "unsigned" : true, "zerofill" : false}},
 *
 */
public class ExpressionMatchingMap<T> {
    private ValueFilter<T> valueFilter;
    private Map<String, T> exactlyMatchMap = new ConcurrentHashMap<>();
    private Map<String, List<TypeExpr<T>>> prefixTypeExprListMap = new ConcurrentHashMap<>();

    public static <T> ExpressionMatchingMap<T> map(String json, TypeHolder<Map<String, T>> typeHolder) {
        return new ExpressionMatchingMap<>(json, typeHolder);
    }

    public static DefaultExpressionMatchingMap map(String json) {
        return new DefaultExpressionMatchingMap(InstanceFactory.instance(JsonParser.class).fromJson(json, new TypeHolder<Map<String, DefaultMap>>(){}));
    }

    public static DefaultExpressionMatchingMap map(Map<String, DefaultMap> map) {
        return new DefaultExpressionMatchingMap(map);
    }

    public ExpressionMatchingMap(String json, TypeHolder<Map<String, T>> typeHolder) {
        this(InstanceFactory.instance(JsonParser.class).fromJson(json, typeHolder));
    }

    public ExpressionMatchingMap(Map<String, T> map) {
        Set<Map.Entry<String, T>> entries = map.entrySet();
        for(Map.Entry<String, T> entry : entries) {
            String key = entry.getKey();
            TypeExpr<T> typeExpr = new TypeExpr<>();
            if(!typeExpr.parseExpression(key))
                continue;
            typeExpr.setValue(entry.getValue());
            int prefixMatchType = typeExpr.getPrefixMatchType();

            switch (prefixMatchType) {
                case TypeExpr.PREFIX_MATCH_ALL:
                    exactlyMatchMap.put(key, entry.getValue());
                    break;
                case TypeExpr.PREFIX_MATCH_START:
                    String prefix = typeExpr.getPrefix();
                    if(prefix == null)
                        break;
                    List<TypeExpr<T>> typeExprList = prefixTypeExprListMap.computeIfAbsent(prefix, k -> new ArrayList<>());
                    if(typeExprList.contains(typeExpr))
                        continue;
                    typeExprList.add(typeExpr);
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * Return the map value, with parameter map under key "_params".
     *
     * @param key
     * @return
     */
    public TypeExprResult<T> get(String key) {
        T value = exactlyMatchMap.get(key);
        if(value == null) {
            Set<String> prefixList = prefixTypeExprListMap.keySet();
            for(String prefix : prefixList) {
                if(!key.startsWith(prefix))
                    continue;
                List<TypeExpr<T>> typeExprList = prefixTypeExprListMap.get(prefix);
                if(typeExprList == null)
                    continue;
                for(TypeExpr<T> typeExpr : typeExprList) {
                    TypeExprResult<T> result = typeExpr.verifyValue(key);
                    if(result == null)
                        continue;
                    valueFilter(result.getValue());
                    return result;
                }
            }
        } else {
            TypeExprResult<T> result = new TypeExprResult<>();
            result.setValue(value);
            valueFilter(value);
            return result;
        }
        return null;
    }

    private void valueFilter(T value) {
        if(valueFilter != null) {
            valueFilter.filter(value);
        }
    }

    public void setValueFilter(ValueFilter<T> valueFilter) {
        this.valueFilter = valueFilter;
    }
}
