package io.tapdata.entity.conversion.impl;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.conversion.TableFieldTypesGenerator;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.mapping.TypeExprResult;
import io.tapdata.entity.mapping.type.TapMapping;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.utils.DataMap;

import java.util.LinkedHashMap;
import java.util.Map;

@Implementation(value = TableFieldTypesGenerator.class, buildNumber = 0)
public class TableFieldTypesGeneratorImpl implements TableFieldTypesGenerator {
    private static final String TAG = TableFieldTypesGenerator.class.getSimpleName();
    @Override
    public void autoFill(LinkedHashMap<String, TapField> nameFieldMap, DefaultExpressionMatchingMap expressionMatchingMap) {
        for(Map.Entry<String, TapField> entry : nameFieldMap.entrySet()) {
            if(entry.getValue().getOriginType() != null) {
                TypeExprResult<DataMap> result = expressionMatchingMap.get(entry.getValue().getOriginType());
                if(result != null) {
                    TapMapping tapMapping = (TapMapping) result.getValue().get(TapMapping.FIELD_TYPE_MAPPING);
                    if(tapMapping != null) {
                        entry.getValue().setTapType(tapMapping.toTapType(entry.getValue().getOriginType(), result.getParams()));
                    }
                } else {
                    TapLogger.error(TAG, "Field originType {} didn't match corresponding TapMapping, please check your dataTypes json definition.", entry.getValue().getOriginType());
                }
            }
        }
    }

    @Override
    public void autoFill(TapField tapField, DefaultExpressionMatchingMap expressionMatchingMap) {
        if(null == tapField) return;
        TypeExprResult<DataMap> result = expressionMatchingMap.get(tapField.getOriginType());
        if(result != null) {
            TapMapping tapMapping = (TapMapping) result.getValue().get(TapMapping.FIELD_TYPE_MAPPING);
            if(tapMapping != null) {
                tapField.setTapType(tapMapping.toTapType(tapField.getOriginType(), result.getParams()));
            }
        } else {
            TapLogger.error(TAG, "Field originType {} didn't match corresponding TapMapping, please check your dataTypes json definition.", tapField.getOriginType());
        }
    }
}
