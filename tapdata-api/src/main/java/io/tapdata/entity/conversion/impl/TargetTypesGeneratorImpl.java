package io.tapdata.entity.conversion.impl;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.codec.filter.TapCodecFilterManager;
import io.tapdata.entity.conversion.TargetTypesGenerator;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.mapping.type.TapMapping;
import io.tapdata.entity.result.ResultItem;
import io.tapdata.entity.result.TapResult;
import io.tapdata.entity.schema.TapField;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Implementation(value = TargetTypesGenerator.class, buildNumber = 0)
public class TargetTypesGeneratorImpl implements TargetTypesGenerator {

    public TapResult<LinkedHashMap<String, TapField>> convert(LinkedHashMap<String, TapField> sourceFields, DefaultExpressionMatchingMap targetMatchingMap, TapCodecFilterManager targetCodecFilterManager) {
        if(sourceFields == null || targetMatchingMap == null)
            return null;
        TapResult<LinkedHashMap<String, TapField>> finalResult = TapResult.successfully();
        LinkedHashMap<String, TapField> targetFieldMap = new LinkedHashMap<>();
        for(Map.Entry<String, TapField> entry : sourceFields.entrySet()) {
            TapField field = entry.getValue();
            TapResult<String> result = calculateBestTypeMapping(field, targetMatchingMap);
            if(result != null) {
                List<ResultItem> resultItems = result.getResultItems();
                if(resultItems != null) {
                    for(ResultItem resultItem : resultItems) {
                        resultItem.setItem(resultItem.getItem() + "@" + field.getName());
                        finalResult.addItem(resultItem);
                    }
                }
            }
            String originType = result != null ? result.getData() : null;
            if((result == null || !result.isSuccessfully()) && field.getTapType() != null) {
                originType = targetCodecFilterManager.getOriginTypeByTapType(field.getTapType().getClass());
                if(originType == null) {
                    //handle by default

                }
            }

            targetFieldMap.put(field.getName(), field.clone().originType(originType));
        }
        if(finalResult.getResultItems() != null && !finalResult.getResultItems().isEmpty()) {
            finalResult.result(TapResult.RESULT_SUCCESSFULLY_WITH_WARN);
        }
        return finalResult.data(targetFieldMap);
    }

    TapResult<String> calculateBestTypeMapping(TapField field, DefaultExpressionMatchingMap matchingMap) {
        AtomicReference<String> hitExpression = new AtomicReference<>();
        AtomicReference<TapMapping> tapMappingReference = new AtomicReference<>();
        AtomicLong bestScore = new AtomicLong(-1);
        matchingMap.iterate(expressionValueEntry -> {
            TapMapping tapMapping = (TapMapping) expressionValueEntry.getValue().get(TapMapping.FIELD_TYPE_MAPPING);
            if(tapMapping != null) {
                if(tapMapping.getQueryOnly() != null && tapMapping.getQueryOnly()) {
                    return false;
                }

                long score = tapMapping.matchingScore(field);
                if(score >= 0 && score > bestScore.get()) {
                    bestScore.set(score);
                    hitExpression.set(expressionValueEntry.getKey());
                    tapMappingReference.set(tapMapping);
                }
                return false;
            }
            return false;
        });
        String expression = hitExpression.get();
        TapMapping tapMapping = tapMappingReference.get();
        if(expression != null && tapMapping != null) {
            return tapMapping.fromTapType(expression, field.getTapType());
        }
        return null;
    }


}
