package io.tapdata.connector.oracle;

import com.google.common.collect.Lists;
import io.tapdata.base.ConnectorBase;
import io.tapdata.connector.oracle.config.OracleConfig;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class OracleConnector extends ConnectorBase {

    private OracleConfig oracleConfig;
    private OracleJdbcContext oracleJdbcContext;
    private String oracleVersion;
    private static final int BATCH_READ_SIZE = 5000;
    private static final int BATCH_ADVANCE_READ_LIMIT = 1000;

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
        //get table info
        List<DataMap> tableList = oracleJdbcContext.queryAllTables(tables);
        List<List<DataMap>> tableLists = Lists.partition(tableList, tableSize);
        tableLists.forEach(subList -> {
            List<TapTable> tapTableList = TapSimplify.list();
            List<String> subTableNames = subList.stream().map(v -> v.getString("TABLE_NAME")).collect(Collectors.toList());
        });
    }

    @Override
    public void connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {

    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        return 0;
    }
}
