package io.tapdata.pdk.core.api;

import io.tapdata.pdk.apis.TapConnectorNode;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionTestResult;
import io.tapdata.pdk.apis.entity.ddl.TapTableOptions;
import io.tapdata.pdk.apis.functions.consumers.TapListConsumer;

public class DatabaseNode extends Node {
    TapConnectorNode connectorNode;
    TapConnectionContext databaseContext;

    public void allTables(TapListConsumer<TapTableOptions> tapReadOffsetConsumer) {
        connectorNode.discoverSchema(databaseContext, tapReadOffsetConsumer);
    }

    public ConnectionTestResult connectionTest() {
        return connectorNode.connectionTest(databaseContext);
    }

    public TapConnectorNode getConnectorNode() {
        return connectorNode;
    }

    public TapConnectionContext getDatabaseContext() {
        return databaseContext;
    }
}
