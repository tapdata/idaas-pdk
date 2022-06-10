package io.tapdata.pdk.apis.functions.connector.common;

import io.tapdata.pdk.apis.context.TapConnectorContext;

public interface ReleaseExternalFunction {
    void release(TapConnectorContext connectorContext) throws Throwable;
}
