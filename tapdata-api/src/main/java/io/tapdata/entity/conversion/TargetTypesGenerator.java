package io.tapdata.entity.conversion;

import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.mapping.type.TapMapping;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.type.TapType;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class TargetTypesGenerator {

    public LinkedHashMap<String, TapField> convert(LinkedHashMap<String, TapField> sourceFields, DefaultExpressionMatchingMap matchingMap) {
        if(sourceFields == null || matchingMap == null)
            return null;
        LinkedHashMap<String, TapField> targetFieldMap = new LinkedHashMap<>();
        for(Map.Entry<String, TapField> entry : sourceFields.entrySet()) {
            TapField field = entry.getValue();
            String originType = calculateBestTypeMapping(field, matchingMap);
            targetFieldMap.put(field.getName(), field.clone().originType(originType));
        }
        return targetFieldMap;
    }

    String calculateBestTypeMapping(TapField field, DefaultExpressionMatchingMap matchingMap) {
        AtomicReference<String> hitTapMapping = new AtomicReference<>();
        matchingMap.iterate(expressionValueEntry -> {
            TapMapping tapMapping = (TapMapping) expressionValueEntry.getValue().get(TapMapping.FIELD_TYPE_MAPPING);
            if(tapMapping != null) {
                if(tapMapping.getQueryOnly() != null && tapMapping.getQueryOnly()) {
                    return false;
                }
                String originType = tapMapping.fromTapType(expressionValueEntry.getKey(), field.getTapType());
                //TODO need better conversion implementation here.
                if(originType != null) {
                    hitTapMapping.set(originType);
                    return true;
                }
            }
            return false;
        });
        return hitTapMapping.get();
    }


}
