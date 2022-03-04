package io.tapdata.pdk.apis.functions;

import io.tapdata.pdk.apis.functions.common.ValueClassFunction;
import io.tapdata.pdk.apis.functions.common.TypeMappingFunction;

public class CommonFunctions<T extends CommonFunctions<?>> implements Functions {
    private TypeMappingFunction typeMappingFunction;
    private ValueClassFunction valueClassFunction;

    public TypeMappingFunction getTypeMappingFunction() {
      return typeMappingFunction;
    }

    public ValueClassFunction getTapValueClassFunction() {
      return valueClassFunction;
    }

    public void setTypeMappingAndTapValueClassFunction(
      TypeMappingFunction typeMappingFunction, ValueClassFunction valueClassFunction) {
        this.typeMappingFunction = typeMappingFunction;
        this.valueClassFunction = valueClassFunction;
    }

}
