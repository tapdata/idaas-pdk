package io.tapdata.pdk.apis.context;

import io.tapdata.entity.utils.DefaultMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
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

    public String toString() {
        return "TapConnectionContext connectionConfig: " + (connectionConfig != null ? InstanceFactory.instance(JsonParser.class).toJson(connectionConfig) : "") + " spec: " + specification;
    }
}
