package io.tapdata.entity.conversion.impl;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.codec.filter.TapCodecFilterManager;
import io.tapdata.entity.conversion.TargetTypesGenerator;
import io.tapdata.entity.conversion.UnsupportedTypeFallbackHandler;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.mapping.type.TapMapping;
import io.tapdata.entity.result.ResultItem;
import io.tapdata.entity.result.TapResult;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapMap;
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.utils.InstanceFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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

            String originType = null;
            //User custom codec
            if(field.getTapType() != null) {
                originType = targetCodecFilterManager.getOriginTypeByTapType(field.getTapType().getClass());
            }

            if(originType == null) {
                //Find best codec
                TapResult<String> result = calculateBestTypeMapping(field, targetMatchingMap);
                if(result != null) {
                    originType = result.getData();
                    List<ResultItem> resultItems = result.getResultItems();
                    if(resultItems != null) {
                        for(ResultItem resultItem : resultItems) {
                            resultItem.setItem(resultItem.getItem() + "@" + field.getName());
                            finalResult.addItem(resultItem);
                        }
                    }
                }
            }

            if(originType == null) {
                //handle by default
                if(cachedLargestStringMapping == null) {
                    cachedTapString = new TapString();
                    cachedLargestStringMapping = findLargestStringType(targetMatchingMap, cachedTapString);
                }
                originType = cachedLargestStringMapping;
                if(originType != null) {
                    UnsupportedTypeFallbackHandler unsupportedTypeFallbackHandler = InstanceFactory.instance(UnsupportedTypeFallbackHandler.class);
                    if(unsupportedTypeFallbackHandler != null) {
                        unsupportedTypeFallbackHandler.handle(targetCodecFilterManager.getCodecRegistry(), field, cachedLargestStringMapping, cachedTapString);
                    }

//                        field.setTapType(cachedTapString);
                }
            }

            targetFieldMap.put(field.getName(), field.clone().originType(originType));
        }
        if(finalResult.getResultItems() != null && !finalResult.getResultItems().isEmpty()) {
            finalResult.result(TapResult.RESULT_SUCCESSFULLY_WITH_WARN);
        }
        return finalResult.data(targetFieldMap);
    }

    private String findLargestStringType(DefaultExpressionMatchingMap targetMatchingMap, TapString tapString) {
        if(targetMatchingMap == null || targetMatchingMap.isEmpty())
            return null;
        HitTapMappingContainer hitTapMapping = new HitTapMappingContainer();
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
                        hitTapMapping.input(expressionValueEntry.getKey(), tapMapping, score);
//                        if(score > hitTapMapping.score) {
//                            hitTapMapping.score = score;
//                            hitTapMapping.hitExpression = expressionValueEntry.getKey();
//                            hitTapMapping.tapMapping = tapMapping;
//                        }
                    }
                }
                return false;
            }
            return false;
        });
        HitTapMapping bestOne = hitTapMapping.getBestOne();
        if(bestOne == null)
            return null;
        TapResult<String> result = bestOne.tapMapping.fromTapType(bestOne.hitExpression, field.getTapType());
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

        public HitTapMapping(String hitExpression, TapMapping tapMapping, long score) {
            this.hitExpression = hitExpression;
            this.tapMapping = tapMapping;
            this.score = score;
        }
    }

    static class HitTapMappingContainer {
        TreeMap<Integer, HitTapMapping> sortedMap = new TreeMap<>();
        HitTapMapping bestOne = null;

        void input(String hitExpression, TapMapping tapMapping, long score) {
            if(bestOne == null || score > bestOne.score) {
                sortedMap.clear();
                bestOne = new HitTapMapping(hitExpression, tapMapping, score);
                sortedMap.put(tapMapping.getPriority(), bestOne);
            } else if(score == bestOne.score) {
                sortedMap.put(tapMapping.getPriority(), new HitTapMapping(hitExpression, tapMapping, score));
            }
        }

        HitTapMapping getBestOne() {
            Map.Entry<Integer, HitTapMapping> entry = sortedMap.firstEntry();
            if(entry != null)
                return entry.getValue();
            return null;
        }
    }

    TapResult<String> calculateBestTypeMapping(TapField field, DefaultExpressionMatchingMap matchingMap) {
        HitTapMappingContainer bestTapMapping = new HitTapMappingContainer();
        HitTapMappingContainer bestNotHitTapMapping = new HitTapMappingContainer();
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
                    bestTapMapping.input(expressionValueEntry.getKey(), tapMapping, score);
//                    if(score > bestTapMapping.score) {
//                        bestTapMapping.score = score;
//                        bestTapMapping.hitExpression = expressionValueEntry.getKey();
//                        bestTapMapping.tapMapping = tapMapping;
//                    }
                } else {
                    bestNotHitTapMapping.input(expressionValueEntry.getKey(), tapMapping, score);
//                    if(score > bestNotHitTapMapping.score) {
//                        bestNotHitTapMapping.score = score;
//                        bestNotHitTapMapping.hitExpression = expressionValueEntry.getKey();
//                        bestNotHitTapMapping.tapMapping = tapMapping;
//                    }
                }
                return false;
            }
            return false;
        });
        HitTapMapping bestOne = bestTapMapping.getBestOne();
        if(bestOne != null && bestOne.tapMapping != null && bestOne.hitExpression != null) {
            return bestOne.tapMapping.fromTapType(bestOne.hitExpression, field.getTapType());
        }
        HitTapMapping notHitBestOne = bestNotHitTapMapping.getBestOne();
        if(notHitBestOne != null && notHitBestOne.tapMapping != null && notHitBestOne.hitExpression != null) {
            TapResult<String> tapResult = notHitBestOne.tapMapping.fromTapType(notHitBestOne.hitExpression, field.getTapType());
            tapResult.addItem(new ResultItem("BEST_IN_UNMATCHED", TapResult.RESULT_SUCCESSFULLY_WITH_WARN, "Select best in unmatched TapMapping, " + notHitBestOne.hitExpression));
            return tapResult;
        }
        return null;
    }


}
