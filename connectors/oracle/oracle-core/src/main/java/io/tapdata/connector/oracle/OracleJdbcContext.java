package io.tapdata.connector.oracle;

import com.zaxxer.hikari.HikariDataSource;
import io.tapdata.common.JdbcContext;
import io.tapdata.connector.oracle.config.OracleConfig;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class OracleJdbcContext extends JdbcContext {

    private final static String TAG = OracleJdbcContext.class.getSimpleName();

    public OracleJdbcContext(OracleConfig config, HikariDataSource hikariDataSource) {
        super(config, hikariDataSource);
    }

    @Override
    public List<DataMap> queryAllTables(List<String> tableNames) {
        TapLogger.debug(TAG, "Query some tables, schema: " + getConfig().getSchema());
        List<DataMap> tableList = TapSimplify.list();
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "WHERE T.TABLE_NAME IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        try {
            query(String.format(ORACLE_ALL_TABLE, tableSql),
                    resultSet -> tableList.addAll(DbKit.getDataFromResultSet(resultSet)));
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllTables failed, error: " + e.getMessage(), e);
        }
        return tableList;
    }

    @Override
    public List<DataMap> queryAllColumns(List<String> tableNames) {
        return null;
    }

    @Override
    public List<DataMap> queryAllIndexes(List<String> tableNames) {
        return null;
    }

    @Override
    public String queryVersion() {
        AtomicReference<String> version = new AtomicReference<>("");
        try {
            query("SELECT * FROM V$VERSION", resultSet -> version.set(resultSet.getString(1)));
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return version.get();
    }

    private final static String ORACLE_ALL_TABLE = "SELECT T.TABLE_NAME, C.COMMENTS\n" +
            "FROM USER_TABLES T\n" +
            "         INNER JOIN\n" +
            "     USER_TAB_COMMENTS C ON C.TABLE_NAME = T.TABLE_NAME %s";
}
