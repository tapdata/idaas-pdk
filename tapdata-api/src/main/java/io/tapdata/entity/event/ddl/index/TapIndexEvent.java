package io.tapdata.entity.event.ddl.index;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.schema.TapIndex;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class TapIndexEvent extends TapDDLEvent {
    private List<TapIndex> indexList;

    public List<TapIndex> getIndexList() {
        return indexList;
    }

    public void setIndexList(List<TapIndex> indexList) {
        this.indexList = indexList;
    }

    @Override
    public void clone(TapEvent tapEvent) {
        super.clone(tapEvent);
        if(tapEvent instanceof TapIndexEvent) {
            TapIndexEvent tapIndexEvent = (TapIndexEvent) tapEvent;
            tapIndexEvent.indexList = new CopyOnWriteArrayList<>(indexList);
        }
    }
}
