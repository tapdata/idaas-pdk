package io.tapdata.connector.oracle;

import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.util.List;
import java.util.function.Consumer;

public class OracleConnector extends ConnectorBase {



    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {

    }

    @Override
    public void onDestroy(TapConnectionContext connectionContext) throws Throwable {

    }

    @Override
    public void onPause(TapConnectionContext connectionContext) throws Throwable {

    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {

    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {

    }

    @Override
    public void connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {

    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        return 0;
    }
}
