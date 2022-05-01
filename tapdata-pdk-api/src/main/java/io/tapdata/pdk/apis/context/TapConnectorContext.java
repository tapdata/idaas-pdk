package io.tapdata.pdk.apis.context;


import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;


public class TapConnectorContext extends TapConnectionContext {

    protected DataMap nodeConfig;

    public TapConnectorContext(TapNodeSpecification specification, DataMap connectionConfig, DataMap nodeConfig) {
        super(specification, connectionConfig);
        this.nodeConfig = nodeConfig;
    }

    public DataMap getNodeConfig() {
        return nodeConfig;
    }

    public void setNodeConfig(DataMap nodeConfig) {
        this.nodeConfig = nodeConfig;
    }

    public String toString() {
        return "TapConnectorContext " + "connectionConfig: " + (connectionConfig != null ? InstanceFactory.instance(JsonParser.class).toJson(connectionConfig) : "") + " nodeConfig: " + (nodeConfig != null ? InstanceFactory.instance(JsonParser.class).toJson(nodeConfig) : "") + " spec: " + specification;
    }
}
