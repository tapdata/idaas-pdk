package io.tapdata.pdk.core.api;

import io.tapdata.entity.codec.TapCodecRegistry;
import io.tapdata.entity.codec.filter.TapCodecFilterManager;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.TapNode;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

public class ConnectorNode extends Node {
    TapConnector connector;
    TapCodecRegistry codecRegistry;
    TapConnectorContext connectorContext;

    ConnectorFunctions connectorFunctions;
    TapCodecFilterManager codecFilterManager;

    public void init(TapConnector tapNode, TapCodecRegistry codecRegistry, ConnectorFunctions connectorFunctions) {
        connector = tapNode;
        this.codecRegistry = codecRegistry;
        this.connectorFunctions = connectorFunctions;
        codecFilterManager = new TapCodecFilterManager(this.codecRegistry);
    }

    public void init(TapConnector tapNode) {
        init(tapNode, new TapCodecRegistry(), new ConnectorFunctions());
    }

    public TapCodecRegistry getCodecRegistry() {
        return codecRegistry;
    }

    public void registerCapabilities() {
        connector.registerCapabilities(connectorFunctions, codecRegistry);
    }

    public TapConnectorContext getConnectorContext() {
        return connectorContext;
    }

    public TapConnector getConnector() {
        return connector;
    }

    public ConnectorFunctions getConnectorFunctions() {
        return connectorFunctions;
    }

    public TapCodecFilterManager getCodecFilterManager() {
        return codecFilterManager;
    }
}
