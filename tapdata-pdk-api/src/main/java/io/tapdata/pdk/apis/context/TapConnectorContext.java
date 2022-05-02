package io.tapdata.pdk.apis.context;


import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;

import java.util.List;


public class TapConnectorContext extends TapConnectionContext {
    protected String table;
    protected List<String> tables;
    protected DataMap nodeConfig;

    public TapConnectorContext(TapNodeSpecification specification, DataMap connectionConfig, DataMap nodeConfig) {
        super(specification, connectionConfig);
        this.nodeConfig = nodeConfig;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List<String> getTables() {
        return tables;
    }

    public void setTables(List<String> tables) {
        this.tables = tables;
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
