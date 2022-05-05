package io.tapdata.entity.event.control;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.TapTable;

import java.util.List;

public class TapForerunnerEvent extends PatrolEvent {
    private TapTable table;
    public TapForerunnerEvent table(TapTable table) {
        this.table = table;
        return this;
    }
    private List<TapInsertRecordEvent> sampleRecords;
    public TapForerunnerEvent sampleRecords(List<TapInsertRecordEvent> sampleRecords) {
        this.sampleRecords = sampleRecords;
        return this;
    }

    @Override
    public void clone(TapEvent tapEvent) {
        super.clone(tapEvent);
        if(tapEvent instanceof TapForerunnerEvent) {
            TapForerunnerEvent tapForerunnerEvent = (TapForerunnerEvent) tapEvent;
            tapForerunnerEvent.table = table; //TODO need copy?
            tapForerunnerEvent.sampleRecords = sampleRecords; //TODO need copy?
        }
    }

    public TapTable getTable() {
        return table;
    }

    public void setTable(TapTable table) {
        this.table = table;
    }

    public List<TapInsertRecordEvent> getSampleRecords() {
        return sampleRecords;
    }

    public void setSampleRecords(List<TapInsertRecordEvent> sampleRecords) {
        this.sampleRecords = sampleRecords;
    }
}
