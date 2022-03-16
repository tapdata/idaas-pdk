package io.tapdata.pdk.apis.context;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.common.DefaultMap;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.apis.utils.ImplementationUtils;

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
        return "TapConnectorContext table: " + (table != null ? table.getName() : "") + " connectionConfig: " + (connectionConfig != null ? ImplementationUtils.getTapUtils().toJson(connectionConfig) : "") + " nodeConfig: " + (nodeConfig != null ? ImplementationUtils.getTapUtils().toJson(nodeConfig) : "") + " spec: " + specification;
    }
}
