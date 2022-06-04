package io.tapdata.connector.oracle;

import com.zaxxer.hikari.HikariDataSource;
import io.tapdata.common.JdbcContext;
import io.tapdata.connector.oracle.config.OracleConfig;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;

import java.util.List;

public class OracleJdbcContext extends JdbcContext {

    private final static String TAG = OracleJdbcContext.class.getSimpleName();

    public OracleJdbcContext(OracleConfig config, HikariDataSource hikariDataSource) {
        super(config, hikariDataSource);
    }

    @Override
    public List<String> queryAllTables(List<String> tableNames) {
        TapLogger.debug(TAG, "Query some tables, schema: " + getConfig().getSchema());
        List<String> tableList = TapSimplify.list();
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "WHERE TABLE_NAME IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        try {
            query(String.format(ORACLE_ALL_TABLE, tableSql), resultSet -> {
                while (!resultSet.isAfterLast() && resultSet.getRow() > 0) {
                    tableList.add(resultSet.getString("TABLE_NAME"));
                    resultSet.next();
                }
            });
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

    private final static String ORACLE_ALL_TABLE = "SELECT TABLE_NAME FROM USER_TABLES %s";
}
