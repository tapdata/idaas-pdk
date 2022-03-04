package io.tapdata.pdk.core.api;

import io.tapdata.pdk.apis.TapSource;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.SourceFunctions;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;

public class SourceNode extends Node {
    TapSource source;
    TapConnectorContext connectorContext;

    SourceFunctions sourceFunctions;

    public void init(TapNodeSpecification specification) {
        source.init(connectorContext, specification);
    }
    public void sourceFunctions(SourceFunctions sourceFunctions) {
        source.sourceFunctions(sourceFunctions);
    }

    public TapConnectorContext getConnectorContext() {
        return connectorContext;
    }

    public TapSource getSource() {
        return source;
    }

    public SourceFunctions getSourceFunctions() {
        return sourceFunctions;
    }

}
