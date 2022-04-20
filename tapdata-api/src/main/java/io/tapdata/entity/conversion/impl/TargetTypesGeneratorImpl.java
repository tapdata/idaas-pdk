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

    static class HitTapMapping {
        String hitExpression;
        TapMapping tapMapping;
        long score = Long.MIN_VALUE;
    }

    TapResult<String> calculateBestTypeMapping(TapField field, DefaultExpressionMatchingMap matchingMap) {
        HitTapMapping bestTapMapping = new HitTapMapping();
        HitTapMapping bestNotHitTapMapping = new HitTapMapping();
//        AtomicReference<String> hitExpression = new AtomicReference<>();
//        AtomicReference<TapMapping> tapMappingReference = new AtomicReference<>();
//        AtomicLong bestScore = new AtomicLong(-1);
        matchingMap.iterate(expressionValueEntry -> {
            TapMapping tapMapping = (TapMapping) expressionValueEntry.getValue().get(TapMapping.FIELD_TYPE_MAPPING);
            if(tapMapping != null) {
                if(tapMapping.getQueryOnly() != null && tapMapping.getQueryOnly()) {
                    return false;
                }

                long score = tapMapping.matchingScore(field);
                if(score >= 0) {
                    if(score > bestTapMapping.score) {
                        bestTapMapping.score = score;
                        bestTapMapping.hitExpression = expressionValueEntry.getKey();
                        bestTapMapping.tapMapping = tapMapping;
                    }
                } else {
                    if(score > bestNotHitTapMapping.score) {
                        bestNotHitTapMapping.score = score;
                        bestNotHitTapMapping.hitExpression = expressionValueEntry.getKey();
                        bestNotHitTapMapping.tapMapping = tapMapping;
                    }
                }
                return false;
            }
            return false;
        });
        if(bestTapMapping.tapMapping != null) {
            if(bestTapMapping.hitExpression != null) {
                return bestTapMapping.tapMapping.fromTapType(bestTapMapping.hitExpression, field.getTapType());
            }
        }
        if(bestNotHitTapMapping.tapMapping != null) {
            if(bestNotHitTapMapping.hitExpression != null) {
                TapResult<String> tapResult = bestNotHitTapMapping.tapMapping.fromTapType(bestNotHitTapMapping.hitExpression, field.getTapType());
                tapResult.addItem(new ResultItem("BEST_IN_UNMATCHED", TapResult.RESULT_SUCCESSFULLY_WITH_WARN, "Select best in unmatched TapMapping, " + bestNotHitTapMapping.hitExpression));
                return tapResult;
            }
        }
        return null;
    }


}
