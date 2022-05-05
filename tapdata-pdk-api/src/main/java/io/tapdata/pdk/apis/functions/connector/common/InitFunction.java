package io.tapdata.pdk.apis.functions.connector.common;

import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapFunction;

public interface InitFunction extends TapFunction {
    void init(TapConnectorContext nodeContext) throws Throwable;
}
