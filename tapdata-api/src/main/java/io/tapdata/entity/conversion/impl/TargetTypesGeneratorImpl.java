package io.tapdata.entity.conversion.impl;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.codec.filter.TapCodecFilterManager;
import io.tapdata.entity.conversion.TargetTypesGenerator;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.mapping.type.TapMapping;
import io.tapdata.entity.schema.TapField;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@Implementation(TargetTypesGenerator.class)
public class TargetTypesGeneratorImpl implements TargetTypesGenerator {

    public LinkedHashMap<String, TapField> convert(LinkedHashMap<String, TapField> sourceFields, DefaultExpressionMatchingMap matchingMap, TapCodecFilterManager codecFilterManager) {
        if(sourceFields == null || matchingMap == null)
            return null;
        LinkedHashMap<String, TapField> targetFieldMap = new LinkedHashMap<>();
        for(Map.Entry<String, TapField> entry : sourceFields.entrySet()) {
            TapField field = entry.getValue();
            String originType = calculateBestTypeMapping(field, matchingMap);
            if(originType == null && field.getTapType() != null) {
                originType = codecFilterManager.getOriginTypeByTapType(field.getTapType().getClass());
            }
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
