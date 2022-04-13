package io.tapdata.entity.conversion;

import io.tapdata.entity.codec.filter.TapCodecFilterManager;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.schema.TapField;

import java.util.LinkedHashMap;

public interface TargetTypesGenerator {
    LinkedHashMap<String, TapField> convert(LinkedHashMap<String, TapField> sourceFields, DefaultExpressionMatchingMap targetMatchingMap, TapCodecFilterManager targetCodecFilterManager);
}
