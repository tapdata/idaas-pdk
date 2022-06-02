package io.tapdata.connector.postgres;

import com.zaxxer.hikari.HikariDataSource;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;

import java.sql.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class PostgresJdbcContext {

    private final static String TAG = PostgresJdbcContext.class.getSimpleName();
    private final HikariDataSource hikariDataSource;
    private final PostgresConfig postgresConfig;
    private final AtomicInteger connectorNumber = new AtomicInteger(1);

    public PostgresJdbcContext incrementAndGet() {
        connectorNumber.incrementAndGet();
        return this;
    }

    public PostgresJdbcContext(PostgresConfig postgresConfig, HikariDataSource hikariDataSource) {
        this.postgresConfig = postgresConfig;
        this.hikariDataSource = hikariDataSource;
    }

    public Connection getConnection() throws SQLException {
        return hikariDataSource.getConnection();
    }

    public static void tryCommit(Connection connection) {
        try {
            if (EmptyKit.isNotNull(connection) && connection.isValid(5) && !connection.getAutoCommit()) {
                connection.commit();
            }
        } catch (Throwable ignored) {
        }
    }

    public static void tryRollBack(Connection connection) {
        try {
            if (EmptyKit.isNotNull(connection) && connection.isValid(5) && !connection.getAutoCommit()) {
                connection.rollback();
            }
        } catch (Throwable ignored) {
        }
    }

    public String queryVersion() {
        AtomicReference<String> version = new AtomicReference<>("");
        try {
            query("SELECT VERSION()", resultSet -> version.set(resultSet.getString(1)));
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return version.get();
    }

    public List<String> queryAllTables(String tableName) {
        TapLogger.debug(TAG, "Query all tables, schema: " + postgresConfig.getSchema());
        List<String> tableList = TapSimplify.list();
        String tableSql = EmptyKit.isNotNull(tableName) ? "AND table_name='" + tableName + "'" : "";
        try {
            query(String.format(PG_ALL_TABLE, postgresConfig.getDatabase(), postgresConfig.getSchema(), tableSql), resultSet -> {
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

    public List<DataMap> queryAllColumns(String tableName) {
        TapLogger.debug(TAG, "Query all columns, schema: " + postgresConfig.getSchema());
        List<DataMap> columnList = TapSimplify.list();
        String tableSql = EmptyKit.isNotNull(tableName) ? "AND table_name='" + tableName + "'" : "";
        try {
            query(String.format(PG_ALL_COLUMN, postgresConfig.getDatabase(), postgresConfig.getSchema(), tableSql),
                    resultSet -> columnList.addAll(DbKit.getDataFromResultSet(resultSet)));
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllColumns failed, error: " + e.getMessage(), e);
        }
        return columnList;
    }

    public List<DataMap> queryAllIndexes(String tableName) {
        TapLogger.debug(TAG, "Query all indexes, schema: " + postgresConfig.getSchema());
        List<DataMap> indexList = TapSimplify.list();
        String tableSql = EmptyKit.isNotNull(tableName) ? "AND table_name='" + tableName + "'" : "";
        try {
            query(String.format(PG_ALL_INDEX, postgresConfig.getDatabase(), postgresConfig.getSchema(), tableSql),
                    resultSet -> indexList.addAll(DbKit.getDataFromResultSet(resultSet)));
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllIndexes failed, error: " + e.getMessage(), e);
        }
        return indexList;
    }

    public void query(String sql, ResultSetConsumer resultSetConsumer) throws Throwable {
        TapLogger.debug(TAG, "Execute query, sql: " + sql);
        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)
        ) {
            if (EmptyKit.isNotNull(resultSet)) {
                resultSet.next();
                resultSetConsumer.accept(resultSet);
            }
        } catch (SQLException e) {
            throw new SQLException("Execute query failed, sql: " + sql + ", code: " + e.getSQLState() + "(" + e.getErrorCode() + "), error: " + e.getMessage(), e);
        }
    }

    public void query(PreparedStatement preparedStatement, ResultSetConsumer resultSetConsumer) throws Throwable {
        TapLogger.debug(TAG, "Execute query, sql: " + preparedStatement);
        try (
                ResultSet resultSet = preparedStatement.executeQuery()
        ) {
            if (EmptyKit.isNotNull(resultSet)) {
                resultSet.next();
                resultSetConsumer.accept(resultSet);
            }
        } catch (SQLException e) {
            throw new SQLException("Execute query failed, sql: " + preparedStatement + ", code: " + e.getSQLState() + "(" + e.getErrorCode() + "), error: " + e.getMessage(), e);
        }
    }

    public void execute(String sql) throws SQLException {
        TapLogger.debug(TAG, "Execute sql: " + sql);
        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement()
        ) {
            statement.execute(sql);
            connection.commit();
        } catch (SQLException e) {
            throw new SQLException("Execute sql failed, sql: " + sql + ", message: " + e.getSQLState() + " " + e.getErrorCode() + " " + e.getMessage(), e);
        }
    }

    public void batchExecute(List<String> sqls) throws SQLException {
        TapLogger.debug(TAG, "batchExecute sqls: " + sqls);
        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement()
        ) {
            for (String sql : sqls) {
                statement.execute(sql);
            }
            connection.commit();
        } catch (SQLException e) {
            throw new SQLException("batchExecute sql failed, sqls: " + sqls + ", message: " + e.getSQLState() + " " + e.getErrorCode() + " " + e.getMessage(), e);
        }
    }

    public void finish() {
        if (connectorNumber.decrementAndGet() <= 0) {
            this.hikariDataSource.close();
            PostgresDataPool.removeJdbcContext(postgresConfig);
        }
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
