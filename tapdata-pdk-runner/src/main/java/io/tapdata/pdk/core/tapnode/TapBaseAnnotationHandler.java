package io.tapdata.pdk.core.tapnode;

import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.reflection.ClassAnnotationHandler;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class TapBaseAnnotationHandler extends ClassAnnotationHandler {
    /**
     * Key is id from TapNodeSpecification
     * Value is TapNodeInfo
     */
    protected Map<String, TapNodeInfo> idGroupTapNodeInfoMap = new ConcurrentHashMap<>();
    protected Map<String, TapNodeInfo> newerIdGroupTapNodeInfoMap;

    public void applyNewerNotInfoMap() {
        if(newerIdGroupTapNodeInfoMap != null) {
            idGroupTapNodeInfoMap = newerIdGroupTapNodeInfoMap;
            newerIdGroupTapNodeInfoMap = null;
        }
    }

    public TapNodeInfo getTapNodeInfo(String id, String group) {
        return getTapNodeInfo(id, group, 0);
    }
    public TapNodeInfo getTapNodeInfo(String id, String group, int minBuildNumber) {
        TapNodeInfo tapNodeInfo = idGroupTapNodeInfoMap.get(TapNodeSpecification.idAndGroup(id, group));
        if(tapNodeInfo != null && tapNodeInfo.getTapNodeSpecification() != null && tapNodeInfo.getTapNodeSpecification().getBuildNumber() != null) {
            if(tapNodeInfo.getTapNodeSpecification().getBuildNumber() >= minBuildNumber)
                return tapNodeInfo;
        }
        return null;
    }

    public Collection<TapNodeInfo> getTapNodeInfos() {
        return idGroupTapNodeInfoMap.values();
    }

}
