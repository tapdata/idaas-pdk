package io.tapdata.entity.event.ddl.index;

import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.schema.TapIndex;

import java.util.List;

public class TapIndexEvent extends TapDDLEvent {
    private List<TapIndex> indexList;

    public List<TapIndex> getIndexList() {
        return indexList;
    }

    public void setIndexList(List<TapIndex> indexList) {
        this.indexList = indexList;
    }
}
