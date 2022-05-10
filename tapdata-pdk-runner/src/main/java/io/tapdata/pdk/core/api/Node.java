package io.tapdata.pdk.core.api;

import io.tapdata.pdk.core.tapnode.TapNodeInfo;

import java.util.List;
import java.util.Map;


public abstract class Node {
    String dagId;
    String associateId;
    TapNodeInfo tapNodeInfo;
    List<Map<String, Object>> tasks;

    public void applyClassLoaderContext() {
        if(tapNodeInfo != null && tapNodeInfo.getNodeClass() != null) {
            Thread.currentThread().setContextClassLoader(tapNodeInfo.getNodeClass().getClassLoader());
        }
    }

    public ClassLoader getConnectorClassLoader() {
        return tapNodeInfo.getNodeClass().getClassLoader();
    }

    public String getDagId() {
        return dagId;
    }

    public String getAssociateId() {
        return associateId;
    }

    public TapNodeInfo getTapNodeInfo() {
        return tapNodeInfo;
    }

    public List<Map<String, Object>> getTasks() {
        return tasks;
    }
}
