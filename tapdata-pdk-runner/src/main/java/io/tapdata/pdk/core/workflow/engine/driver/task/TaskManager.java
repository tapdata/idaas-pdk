package io.tapdata.pdk.core.workflow.engine.driver.task;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class TaskManager {
    private List<Task.TableFilter> tableFilters = new CopyOnWriteArrayList<>();
    public void init(List<Map<String, Object>> tasks) {
        if(tasks == null)
            return;
        for(Map<String, Object> taskMap : tasks) {
            Task task = Task.build(taskMap);
            if(task != null) {
                Task.TableFilter tableFilter = task.getTableFilter();
                if(tableFilter != null && !tableFilters.contains(tableFilter))
                    tableFilters.add(tableFilter);
            }
        }
    }
}
