package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.event.ddl.entity.FieldAttrChange;

import java.util.List;

public interface AlterFieldNamesFunction {
    void alterFieldName(List<FieldAttrChange<String>> fieldNameChangeList);
}
