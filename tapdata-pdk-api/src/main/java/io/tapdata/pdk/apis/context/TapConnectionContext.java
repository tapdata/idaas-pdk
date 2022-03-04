package io.tapdata.pdk.apis.context;

import io.tapdata.pdk.apis.common.DefaultMap;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;

public class TapConnectionContext extends TapContext {
    protected DefaultMap connectionConfig;

    public TapConnectionContext(TapNodeSpecification specification, DefaultMap connectionConfig) {
        super(specification);
        this.connectionConfig = connectionConfig;
    }

    public DefaultMap getConnectionConfig() {
        return connectionConfig;
    }

    public void setConnectionConfig(DefaultMap connectionConfig) {
        this.connectionConfig = connectionConfig;
    }
}
