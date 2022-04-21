package io.tapdata.entity.conversion.impl;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.codec.filter.TapCodecFilterManager;
import io.tapdata.entity.conversion.TargetTypesGenerator;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.mapping.type.TapMapping;
import io.tapdata.entity.result.ResultItem;
import io.tapdata.entity.result.TapResult;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapString;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Implementation(value = TargetTypesGenerator.class, buildNumber = 0)
public class TargetTypesGeneratorImpl implements TargetTypesGenerator {
    private static final String TAG = TargetTypesGeneratorImpl.class.getSimpleName();

    public TapResult<LinkedHashMap<String, TapField>> convert(LinkedHashMap<String, TapField> sourceFields, DefaultExpressionMatchingMap targetMatchingMap, TapCodecFilterManager targetCodecFilterManager) {
        if(sourceFields == null || targetMatchingMap == null)
            return null;
        TapResult<LinkedHashMap<String, TapField>> finalResult = TapResult.successfully();
        LinkedHashMap<String, TapField> targetFieldMap = new LinkedHashMap<>();
        String cachedLargestStringMapping = null;
        TapString cachedTapString = null;

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
                    if(cachedLargestStringMapping == null) {
                        cachedTapString = new TapString();
                        cachedLargestStringMapping = findLargestStringMapping(targetMatchingMap, cachedTapString);
                    }
                    originType = cachedLargestStringMapping;
                    if(originType != null) {
                        field.setTapType(cachedTapString);
                    }
                }
            }

            targetFieldMap.put(field.getName(), field.clone().originType(originType));
        }
        if(finalResult.getResultItems() != null && !finalResult.getResultItems().isEmpty()) {
            finalResult.result(TapResult.RESULT_SUCCESSFULLY_WITH_WARN);
        }
        return finalResult.data(targetFieldMap);
    }

    private String findLargestStringMapping(DefaultExpressionMatchingMap targetMatchingMap, TapString tapString) {
        if(targetMatchingMap == null || targetMatchingMap.isEmpty())
            return null;
        HitTapMapping hitTapMapping = new HitTapMapping();
        TapField field = new TapField().tapType(tapString).originType("LargestString");
        targetMatchingMap.iterate(expressionValueEntry -> {
            TapMapping tapMapping = (TapMapping) expressionValueEntry.getValue().get(TapMapping.FIELD_TYPE_MAPPING);
            if(tapMapping != null && tapMapping.getTo() != null) {
                if(tapMapping.getQueryOnly() != null && tapMapping.getQueryOnly()) {
                    return false;
                }
                if(tapMapping.getTo().equals("TapString")) {
                    long score = tapMapping.matchingScore(field);
                    if(score >= 0) {
                        if(score > hitTapMapping.score) {
                            hitTapMapping.score = score;
                            hitTapMapping.hitExpression = expressionValueEntry.getKey();
                            hitTapMapping.tapMapping = tapMapping;
                        }
                    }
                }
                return false;
            }
            return false;
        });
        if(hitTapMapping.tapMapping == null)
            return null;
        TapResult<String> result = hitTapMapping.tapMapping.fromTapType(hitTapMapping.hitExpression, field.getTapType());
        List<ResultItem> resultItems = result.getResultItems();
        if(resultItems != null && !resultItems.isEmpty()) {
            for(ResultItem resultItem : resultItems) {
                TapLogger.warn(TAG, "findLargestStringMapping " + resultItem.getItem() + ": " + resultItem.getInformation());
            }
        }
        return result.getData();
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
        if(bestTapMapping.tapMapping != null && bestTapMapping.hitExpression != null) {
            return bestTapMapping.tapMapping.fromTapType(bestTapMapping.hitExpression, field.getTapType());
        }
        if(bestNotHitTapMapping.tapMapping != null && bestNotHitTapMapping.hitExpression != null) {
            TapResult<String> tapResult = bestNotHitTapMapping.tapMapping.fromTapType(bestNotHitTapMapping.hitExpression, field.getTapType());
            tapResult.addItem(new ResultItem("BEST_IN_UNMATCHED", TapResult.RESULT_SUCCESSFULLY_WITH_WARN, "Select best in unmatched TapMapping, " + bestNotHitTapMapping.hitExpression));
            return tapResult;
        }
        return null;
    }


}
