package io.tapdata.entity.event;

import io.tapdata.entity.schema.TapTable;

public abstract class TapBaseEvent extends TapEvent {
    protected TapTable table;
    /**
     * The reference time read from source, maybe some difference as sources are different
     * Used for CDC in most cases.
     * <p>
     * For example, MongoDB as source, when initial stage, referenceTime is null, when cdc stage, referenceTime is the clusterTime read from CDC stream
     */
    protected Long referenceTime;

    protected String pdkId;
    protected String pdkGroup;
    protected String pdkVersion;

    public void clone(TapBaseEvent tapBaseEvent) {
        super.clone(tapBaseEvent);
        tapBaseEvent.referenceTime = referenceTime;
        tapBaseEvent.pdkId = pdkId;
        tapBaseEvent.pdkGroup = pdkGroup;
        tapBaseEvent.pdkVersion = pdkVersion;
        tapBaseEvent.table = table;
    }

    public Long getReferenceTime() {
        return referenceTime;
    }

    public void setReferenceTime(Long referenceTime) {
        this.referenceTime = referenceTime;
    }

    public String getPdkId() {
        return pdkId;
    }

    public void setPdkId(String pdkId) {
        this.pdkId = pdkId;
    }

    public String getPdkGroup() {
        return pdkGroup;
    }

    public void setPdkGroup(String pdkGroup) {
        this.pdkGroup = pdkGroup;
    }

    public String getPdkVersion() {
        return pdkVersion;
    }

    public void setPdkVersion(String pdkVersion) {
        this.pdkVersion = pdkVersion;
    }

    public TapTable getTable() {
        return table;
    }

    public void setTable(TapTable table) {
        this.table = table;
    }
}
