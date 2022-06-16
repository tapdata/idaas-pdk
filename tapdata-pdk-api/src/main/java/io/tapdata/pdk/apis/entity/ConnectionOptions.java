package io.tapdata.pdk.apis.entity;

import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.CopyOnWriteArrayList;

public class ConnectionOptions {
    /**
     * Database timezone
     */
    private TimeZone timeZone;
    public ConnectionOptions timeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
        return this;
    }
    private List<String> pdkExpansion;
    public ConnectionOptions addExpansion(String expansion) {
        if(pdkExpansion == null)
            pdkExpansion = new CopyOnWriteArrayList<>();
        pdkExpansion.add(expansion);
        return this;
    }

    public static ConnectionOptions create() {
        return new ConnectionOptions();
    }

    public List<String> getPdkExpansion() {
        return pdkExpansion;
    }

    public void setPdkExpansion(List<String> pdkExpansion) {
        this.pdkExpansion = pdkExpansion;
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
    }
}
