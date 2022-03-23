package io.tapdata.pdk.apis.context;

import io.tapdata.entity.utils.DefaultMap;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;

public class TapProcessorContext extends TapContext {
    private DefaultMap nodeConfig;

    public TapProcessorContext(TapNodeSpecification specification, DefaultMap nodeConfig) {
        super(specification);
        this.nodeConfig = nodeConfig;
    }

    public DefaultMap getNodeConfig() {
        return nodeConfig;
    }

    public void setNodeConfig(DefaultMap nodeConfig) {
        this.nodeConfig = nodeConfig;
    }
}
