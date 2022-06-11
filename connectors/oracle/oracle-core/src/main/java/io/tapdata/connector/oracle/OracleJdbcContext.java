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
        TapLogger.debug(TAG, "Query columns of some tables, schema: " + getConfig().getSchema());
        List<DataMap> columnList = TapSimplify.list();
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "WHERE COL.TABLE_NAME IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        try {
            query(String.format(ORACLE_ALL_COLUMN, tableSql),
                    resultSet -> columnList.addAll(DbKit.getDataFromResultSet(resultSet)));
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllColumns failed, error: " + e.getMessage(), e);
        }
        return columnList;
    }

    @Override
    public List<DataMap> queryAllIndexes(List<String> tableNames) {
        TapLogger.debug(TAG, "Query indexes of some tables, schema: " + getConfig().getSchema());
        List<DataMap> indexList = TapSimplify.list();
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "WHERE I.TABLE_NAME IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        try {
            query(String.format(ORACLE_ALL_INDEX, tableSql),
                    resultSet -> indexList.addAll(DbKit.getDataFromResultSet(resultSet)));
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllIndexes failed, error: " + e.getMessage(), e);
        }
        return indexList;
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

    private final static String ORACLE_ALL_COLUMN = "SELECT COL.*, COM.COMMENTS\n" +
            "FROM USER_TAB_COLUMNS COL\n" +
            "INNER JOIN USER_COL_COMMENTS COM\n" +
            "    ON COL.TABLE_NAME=COM.TABLE_NAME\n" +
            "    AND COL.COLUMN_NAME=COM.COLUMN_NAME\n" +
            "%s ORDER BY COL.TABLE_NAME, COL.COLUMN_ID";

    private final static String ORACLE_ALL_INDEX = "SELECT I.TABLE_NAME, I.INDEX_NAME, C.COLUMN_NAME, I.UNIQUENESS, C.DESCEND,\n" +
            "       (CASE WHEN EXISTS (SELECT * FROM USER_CONSTRAINTS WHERE INDEX_NAME=I.INDEX_NAME AND CONSTRAINT_TYPE='P') THEN 1 ELSE 0 END) IS_PK\n" +
            "FROM USER_INDEXES I\n" +
            "         INNER JOIN USER_IND_COLUMNS C ON I.INDEX_NAME = C.INDEX_NAME %s\n" +
            "ORDER BY I.TABLE_NAME, I.INDEX_NAME, C.COLUMN_POSITION";
}
