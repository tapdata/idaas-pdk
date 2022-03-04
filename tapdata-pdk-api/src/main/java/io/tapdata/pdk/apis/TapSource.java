package io.tapdata.pdk.apis;

import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.SourceFunctions;

public interface TapSource extends TapConnectorNode {
    void init(TapConnectorContext nodeContext, TapNodeSpecification specification);
    void sourceFunctions(SourceFunctions sourceFunctions);
}
