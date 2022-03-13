package io.tapdata.pdk.core.api;

import io.tapdata.pdk.core.tapnode.TapNodeInfo;


public abstract class Node {
    String dagId;
    String associateId;
    TapNodeInfo tapNodeInfo;

    public String getDagId() {
        return dagId;
    }

    public String getAssociateId() {
        return associateId;
    }

    public TapNodeInfo getTapNodeInfo() {
        return tapNodeInfo;
    }
}
