package io.tapdata.pdk.apis.context;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DefaultMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;

public class TapConnectorContext extends TapConnectionContext {
    protected DefaultMap nodeConfig;
    private TapTable table;
    public TapConnectorContext(TapNodeSpecification specification, TapTable table, DefaultMap connectionConfig, DefaultMap nodeConfig) {
        super(specification, connectionConfig);
        this.table = table;
        this.nodeConfig = nodeConfig;
    }

    public DefaultMap getNodeConfig() {
        return nodeConfig;
    }

    public void setNodeConfig(DefaultMap nodeConfig) {
        this.nodeConfig = nodeConfig;
    }

    public TapTable getTable() {
        return table;
    }

    public void setTable(TapTable table) {
        this.table = table;
    }

    public String toString() {
        return "TapConnectorContext table: " + (table != null ? table.getName() : "") + " connectionConfig: " + (connectionConfig != null ? InstanceFactory.instance(JsonParser.class).toJson(connectionConfig) : "") + " nodeConfig: " + (nodeConfig != null ? InstanceFactory.instance(JsonParser.class).toJson(nodeConfig) : "") + " spec: " + specification;
    }
}
