package io.tapdata.pdk.apis.context;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;

public class TapConnectorContext extends TapConnectionContext {
    protected DataMap nodeConfig;
    private TapTable table;
    public TapConnectorContext(TapNodeSpecification specification, TapTable table, DataMap connectionConfig, DataMap nodeConfig) {
        super(specification, connectionConfig);
        this.table = table;
        this.nodeConfig = nodeConfig;
    }

    public DataMap getNodeConfig() {
        return nodeConfig;
    }

    public void setNodeConfig(DataMap nodeConfig) {
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
