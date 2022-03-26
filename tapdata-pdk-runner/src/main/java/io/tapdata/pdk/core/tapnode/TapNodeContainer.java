package io.tapdata.pdk.core.tapnode;

import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;

import java.util.Map;

public class TapNodeContainer {
    private TapNodeSpecification properties;

    private DataMap configOptions;

    private Map<String, DataMap> dataTypes;

    public DataMap getConfigOptions() {
        return configOptions;
    }

    public void setConfigOptions(DataMap configOptions) {
        this.configOptions = configOptions;
    }

    public TapNodeSpecification getProperties() {
        return properties;
    }

    public void setProperties(TapNodeSpecification properties) {
        this.properties = properties;
    }

    public Map<String, DataMap> getDataTypes() {
        return dataTypes;
    }

    public void setDataTypes(Map<String, DataMap> dataTypes) {
        this.dataTypes = dataTypes;
    }
}
