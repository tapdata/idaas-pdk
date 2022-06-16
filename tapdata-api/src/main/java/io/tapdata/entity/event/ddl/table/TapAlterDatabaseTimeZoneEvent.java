package io.tapdata.entity.event.ddl.table;

import java.util.TimeZone;

public class TapAlterDatabaseTimeZoneEvent extends TapTableEvent {
    public static final int TYPE = 310;
    private TimeZone timeZone;

    public TapAlterDatabaseTimeZoneEvent() {
        super(TYPE);
    }
}
