package io.tapdata.pdk.core.workflow.engine.driver.task;

import io.tapdata.entity.schema.TapTable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ChangeTableNameTask extends Task implements Task.TableFilter {
    private Map<String, String> nameChangeToMap = new ConcurrentHashMap<>();
    @Override
    protected void from(Map<String, Object> info) {
        Object tablesObj = info.get("tables");
        if(tablesObj instanceof List) {
            List<Object> tableList = (List<Object>) tablesObj;
            for(Object tableObj : tableList) {
                if(!(tableObj instanceof Map)) continue;
                Map<String, Object> tableMap = (Map<String, Object>) tableObj;
                Object fromObj = tableMap.get("from");
                Object toObj = tableMap.get("to");
                if((fromObj instanceof String) && (toObj instanceof String)) {
                    nameChangeToMap.put((String)fromObj, (String)toObj);
                }
            }
        }

        if(!nameChangeToMap.isEmpty()) {
            supportTableFilter(this);
        }
    }

    @Override
    public void table(TapTable table) {
        String newName = nameChangeToMap.get(table.getId());
        if(newName != null) {
            if(table.getName().equals(table.getId())) {
              table.setName(newName);
            }
            table.setId(newName);
        }
    }
}
