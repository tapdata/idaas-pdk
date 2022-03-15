package io.tapdata.pdk.core.tapnode;

import io.tapdata.pdk.apis.spec.TapNodeSpecification;

import java.util.Map;

public class TapNodeContainer {
    private TapNodeSpecification properties;

    private Map<String, Object> configOptions;

    public Map<String, Object> getConfigOptions() {
        return configOptions;
    }

    public void setConfigOptions(Map<String, Object> configOptions) {
        this.configOptions = configOptions;
    }

    public TapNodeSpecification getProperties() {
        return properties;
    }

    public void setProperties(TapNodeSpecification properties) {
        this.properties = properties;
    }

}
