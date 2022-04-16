package io.tapdata.entity.conversion.impl;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.codec.filter.TapCodecFilterManager;
import io.tapdata.entity.conversion.TargetTypesGenerator;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.mapping.type.TapMapping;
import io.tapdata.entity.schema.TapField;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Implementation(value = TargetTypesGenerator.class, buildNumber = 0)
public class TargetTypesGeneratorImpl implements TargetTypesGenerator {

    public LinkedHashMap<String, TapField> convert(LinkedHashMap<String, TapField> sourceFields, DefaultExpressionMatchingMap targetMatchingMap, TapCodecFilterManager targetCodecFilterManager) {
        if(sourceFields == null || targetMatchingMap == null)
            return null;
        LinkedHashMap<String, TapField> targetFieldMap = new LinkedHashMap<>();
        for(Map.Entry<String, TapField> entry : sourceFields.entrySet()) {
            TapField field = entry.getValue();
            String originType = calculateBestTypeMapping(field, targetMatchingMap);
            if(originType == null && field.getTapType() != null) {
                originType = targetCodecFilterManager.getOriginTypeByTapType(field.getTapType().getClass());
            }
            targetFieldMap.put(field.getName(), field.clone().originType(originType));
        }
        return targetFieldMap;
    }

    String calculateBestTypeMapping(TapField field, DefaultExpressionMatchingMap matchingMap) {
        AtomicReference<String> hitTapMapping = new AtomicReference<>();
        AtomicLong bestScore = new AtomicLong(-1);
        matchingMap.iterate(expressionValueEntry -> {
            TapMapping tapMapping = (TapMapping) expressionValueEntry.getValue().get(TapMapping.FIELD_TYPE_MAPPING);
            if(tapMapping != null) {
                if(tapMapping.getQueryOnly() != null && tapMapping.getQueryOnly()) {
                    return false;
                }
                String originType = tapMapping.fromTapType(expressionValueEntry.getKey(), field.getTapType());
                //TODO need better conversion implementation here.
                if(originType != null) {
                    long score = tapMapping.matchingScore(field);
                    if(score >= 0 && score > bestScore.get()) {
                        bestScore.set(score);
                        hitTapMapping.set(originType);
                    }
                    return false;
                }
            }
            return false;
        });
        return hitTapMapping.get();
    }


}
