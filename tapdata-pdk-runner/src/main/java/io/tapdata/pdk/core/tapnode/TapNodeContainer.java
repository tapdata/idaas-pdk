package io.tapdata.pdk.core.tapnode;

import io.tapdata.entity.utils.DefaultMap;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;

import java.util.Map;

public class TapNodeContainer {
    private TapNodeSpecification properties;

    private DefaultMap configOptions;

    private Map<String, DefaultMap> dataTypes;

    public DefaultMap getConfigOptions() {
        return configOptions;
    }

    public void setConfigOptions(DefaultMap configOptions) {
        this.configOptions = configOptions;
    }

    public TapNodeSpecification getProperties() {
        return properties;
    }

    public void setProperties(TapNodeSpecification properties) {
        this.properties = properties;
    }

    public Map<String, DefaultMap> getDataTypes() {
        return dataTypes;
    }

    public void setDataTypes(Map<String, DefaultMap> dataTypes) {
        this.dataTypes = dataTypes;
    }
}
