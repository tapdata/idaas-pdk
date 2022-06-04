package io.tapdata.connector.postgres;

import com.zaxxer.hikari.HikariDataSource;
import io.tapdata.common.JdbcContext;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;

import java.util.List;

public class PostgresJdbcContext extends JdbcContext {

    private final static String TAG = PostgresJdbcContext.class.getSimpleName();

    public PostgresJdbcContext(PostgresConfig config, HikariDataSource hikariDataSource) {
        super(config, hikariDataSource);
    }

    @Override
    public List<String> queryAllTables(List<String> tableNames) {
        TapLogger.debug(TAG, "Query some tables, schema: " + getConfig().getSchema());
        List<String> tableList = TapSimplify.list();
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND table_name IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        try {
            query(String.format(PG_ALL_TABLE, getConfig().getDatabase(), getConfig().getSchema(), tableSql), resultSet -> {
                while (!resultSet.isAfterLast() && resultSet.getRow() > 0) {
                    tableList.add(resultSet.getString("table_name"));
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
        TapLogger.debug(TAG, "Query some columns, schema: " + getConfig().getSchema());
        List<DataMap> columnList = TapSimplify.list();
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND table_name IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        try {
            query(String.format(PG_ALL_COLUMN, getConfig().getDatabase(), getConfig().getSchema(), tableSql),
                    resultSet -> columnList.addAll(DbKit.getDataFromResultSet(resultSet)));
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllColumns failed, error: " + e.getMessage(), e);
        }
        return columnList;
    }

    @Override
    public List<DataMap> queryAllIndexes(List<String> tableNames) {
        TapLogger.debug(TAG, "Query some indexes, schema: " + getConfig().getSchema());
        List<DataMap> indexList = TapSimplify.list();
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND table_name IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        try {
            query(String.format(PG_ALL_INDEX, getConfig().getDatabase(), getConfig().getSchema(), tableSql),
                    resultSet -> indexList.addAll(DbKit.getDataFromResultSet(resultSet)));
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllIndexes failed, error: " + e.getMessage(), e);
        }
        return indexList;
    }

    private final static String PG_ALL_TABLE =
            "SELECT * FROM information_schema.tables WHERE table_catalog='%s' AND table_schema='%s' %s ORDER BY table_name";
    private final static String PG_ALL_COLUMN =
            "SELECT col.*, d.description,\n" +
                    "       (SELECT pg_catalog.format_type(a.atttypid, a.atttypmod) AS \"dataType\"\n" +
                    "        FROM pg_catalog.pg_attribute a\n" +
                    "        WHERE a.attnum > 0\n" +
                    "          AND a.attname = col.column_name\n" +
                    "          AND NOT a.attisdropped\n" +
                    "          AND a.attrelid =\n" +
                    "              (SELECT cl.oid\n" +
                    "               FROM pg_catalog.pg_class cl\n" +
                    "                        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = cl.relnamespace\n" +
                    "               WHERE cl.relname = col.table_name))\n" +
                    "FROM information_schema.columns col\n" +
                    "         JOIN pg_class c ON c.relname = col.table_name\n" +
                    "         LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = col.ordinal_position\n" +
                    "WHERE col.table_catalog='%s' AND col.table_schema='%s' %s\n" +
                    "ORDER BY col.table_name,col.ordinal_position";
    private final static String PG_ALL_INDEX =
            "SELECT\n" +
                    "    t.relname AS table_name,\n" +
                    "    i.relname AS index_name,\n" +
                    "    a.attname AS column_name,\n" +
                    "    ix.indisunique AS is_unique,\n" +
                    "    ix.indisprimary AS is_primary,\n" +
                    "    (CASE WHEN ix.indoption[a.attnum-1]&1=0 THEN 'A' ELSE 'D' END) AS asc_or_desc\n" +
                    "FROM\n" +
                    "    pg_class t,\n" +
                    "    pg_class i,\n" +
                    "    pg_index ix,\n" +
                    "    pg_attribute a,\n" +
                    "    information_schema.tables tt\n" +
                    "WHERE\n" +
                    "        t.oid = ix.indrelid\n" +
                    "  AND i.oid = ix.indexrelid\n" +
                    "  AND a.attrelid = t.oid\n" +
                    "  AND a.attnum = ANY(ix.indkey)\n" +
                    "  AND t.relkind = 'r'\n" +
                    "  AND tt.table_name=t.relname\n" +
                    "  AND tt.table_catalog='%s'\n" +
                    "  AND tt.table_schema='%s'\n" +
                    "    %s\n" +
                    "ORDER BY t.relname, i.relname, a.attnum";
}
