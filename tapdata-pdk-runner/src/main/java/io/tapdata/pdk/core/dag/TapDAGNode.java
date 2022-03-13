package io.tapdata.pdk.core.dag;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.common.DefaultMap;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;

import java.util.List;

public class TapDAGNode {
    protected DefaultMap nodeConfig;
    protected DefaultMap connectionConfig;
    protected String id;
    protected String pdkId;
    protected String group;
    protected String version;
    public static final String TYPE_TARGET = TapNodeInfo.NODE_TYPE_TARGET;
    public static final String TYPE_SOURCE = TapNodeInfo.NODE_TYPE_SOURCE;
    public static final String TYPE_PROCESSOR = TapNodeInfo.NODE_TYPE_PROCESSOR;
    public static final String TYPE_SOURCE_TARGET = TapNodeInfo.NODE_TYPE_SOURCE_TARGET;
    protected String type;
    protected TapTable table;
    protected List<String> parentNodeIds;
    protected List<String> childNodeIds;

    @Override
    public String toString() {
        return type + " " + id + ": " + (table != null ? table.getName() + " on " : "") + pdkId + "@" + group + "-v" + version;
    }

    public String verify() {
        if(id == null)
            return "missing id";
        if(pdkId == null)
            return "missing pdkId";
        if(group == null)
            return "missing group";
        if(type == null)
            return "missing type";
        if(version == null)
            return "missing version";
        if(!type.equals(TYPE_PROCESSOR) && table == null)
            return "missing table";
        if(!type.equals(TYPE_PROCESSOR) && table.getName() == null)
            return "missing table name";
        return null;
    }

    public TapTable getTable() {
        return table;
    }

    public void setTable(TapTable table) {
        this.table = table;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getPdkId() {
        return pdkId;
    }

    public void setPdkId(String pdkId) {
        this.pdkId = pdkId;
    }

    public List<String> getParentNodeIds() {
        return parentNodeIds;
    }

    public void setParentNodeIds(List<String> parentNodeIds) {
        this.parentNodeIds = parentNodeIds;
    }

    public List<String> getChildNodeIds() {
        return childNodeIds;
    }

    public void setChildNodeIds(List<String> childNodeIds) {
        this.childNodeIds = childNodeIds;
    }

    public DefaultMap getNodeConfig() {
        return nodeConfig;
    }

    public void setNodeConfig(DefaultMap nodeConfig) {
        this.nodeConfig = nodeConfig;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public DefaultMap getConnectionConfig() {
        return connectionConfig;
    }

    public void setConnectionConfig(DefaultMap connectionConfig) {
        this.connectionConfig = connectionConfig;
    }
}
