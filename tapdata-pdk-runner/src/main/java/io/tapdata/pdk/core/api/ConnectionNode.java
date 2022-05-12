package io.tapdata.pdk.core.api;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.TapConnectorNode;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.TestItem;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

public class ConnectionNode extends Node {
    TapConnectorNode connectorNode;
    TapConnectionContext connectionContext;

    public void discoverSchema(List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        applyClassLoaderContext();
        connectorNode.discoverSchema(connectionContext, tables, tableSize, consumer);
    }
    public int tableCount() throws Throwable {
        applyClassLoaderContext();
        return connectorNode.tableCount(connectionContext);
    }
    public void connectionTest(Consumer<TestItem> consumer) throws Throwable {
        applyClassLoaderContext();
        connectorNode.connectionTest(connectionContext, consumer);
    }
    public void connectorInit() throws Throwable {
        applyClassLoaderContext();
        connectorNode.init(connectionContext);
    }
    public TapConnectorNode getConnectorNode() {
        return connectorNode;
    }

    public TapConnectionContext getConnectionContext() {
        return connectionContext;
    }
}
