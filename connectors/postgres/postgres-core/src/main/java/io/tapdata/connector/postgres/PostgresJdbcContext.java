package io.tapdata.connector.postgres;

import com.zaxxer.hikari.HikariDataSource;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.connector.postgres.kit.DbKit;
import io.tapdata.connector.postgres.kit.EmptyKit;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.sql.*;
import java.util.List;

public class PostgresJdbcContext implements AutoCloseable {

    private final static String TAG = PostgresJdbcContext.class.getSimpleName();
    private final HikariDataSource hikariDataSource;
    private PostgresConfig postgresConfig;

    public PostgresJdbcContext(TapConnectionContext tapConnectionContext) {
        DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
        PostgresConfig postgresConfig = PostgresConfig.load(connectionConfig);
        hikariDataSource = HikariConnection.getHikariDataSource(postgresConfig);
    }

    public PostgresJdbcContext(PostgresConfig postgresConfig) {
        this.postgresConfig = postgresConfig;
        hikariDataSource = HikariConnection.getHikariDataSource(postgresConfig);
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
            throw new Exception("Execute query failed, sql: " + sql + ", code: " + e.getSQLState() + "(" + e.getErrorCode() + "), error: " + e.getMessage(), e);
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
            throw new Exception("Execute query failed, sql: " + preparedStatement + ", code: " + e.getSQLState() + "(" + e.getErrorCode() + "), error: " + e.getMessage(), e);
        }
    }

    public void execute(String sql) throws Throwable {
        TapLogger.debug(TAG, "Execute sql: " + sql);
        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement()
        ) {
//            connection.setAutoCommit(true);
            statement.execute(sql);
        } catch (SQLException e) {
            throw new Exception("Execute sql failed, sql: " + sql + ", message: " + e.getSQLState() + " " + e.getErrorCode() + " " + e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        if (EmptyKit.isNotNull(hikariDataSource)) {
            hikariDataSource.close();
        }
    }

    static class HikariConnection {
        public static HikariDataSource getHikariDataSource(PostgresConfig postgresConfig) throws IllegalArgumentException {
            HikariDataSource hikariDataSource;
            if (EmptyKit.isNull(postgresConfig)) {
                throw new IllegalArgumentException("Postgres Config cannot be null");
            }
            hikariDataSource = new HikariDataSource();
            hikariDataSource.setDriverClassName("org.postgresql.Driver");
            hikariDataSource.setJdbcUrl(postgresConfig.getDatabaseUrl());
            hikariDataSource.setUsername(postgresConfig.getUser());
            hikariDataSource.setPassword(postgresConfig.getPassword());
            hikariDataSource.setMinimumIdle(1);
            hikariDataSource.setMaximumPoolSize(20);
            hikariDataSource.setAutoCommit(true);
            hikariDataSource.setIdleTimeout(60 * 1000L);
            hikariDataSource.setKeepaliveTime(60 * 1000L);
            return hikariDataSource;
        }
    }

    private final static String PG_ALL_TABLE =
            "SELECT * FROM information_schema.tables WHERE table_catalog='%s' AND table_schema='%s' %s ORDER BY table_name";
    private final static String PG_ALL_COLUMN =
            "select col.*,\n" +
                    "       d.description,\n" +
                    "       (select pg_catalog.format_type(a.atttypid, a.atttypmod) as \"dataType\"\n" +
                    "        from pg_catalog.pg_attribute a\n" +
                    "        where a.attnum > 0\n" +
                    "          and a.attname = col.column_name\n" +
                    "          and not a.attisdropped\n" +
                    "          and a.attrelid = (select cl.oid\n" +
                    "                            from pg_catalog.pg_class cl\n" +
                    "                                     left join pg_catalog.pg_namespace n on n.oid = cl.relnamespace\n" +
                    "                            where cl.relname = col.table_name\n" +
                    "                              and pg_catalog.pg_table_is_visible(cl.oid)))\n" +
                    "from information_schema.columns col\n" +
                    "         join pg_class c on c.relname = col.table_name\n" +
                    "         left join pg_description d on d.objoid = c.oid and d.objsubid = col.ordinal_position\n" +
                    "    WHERE col.table_catalog='%s' AND col.table_schema='%s' %s ORDER BY col.table_name,col.ordinal_position";
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
                    "    t.oid = ix.indrelid\n" +
                    "    AND i.oid = ix.indexrelid\n" +
                    "    AND a.attrelid = t.oid\n" +
                    "    AND a.attnum = ANY(ix.indkey)\n" +
                    "    AND t.relkind = 'r'\n" +
                    "    AND tt.table_name=t.relname\n" +
                    "    AND tt.table_catalog='%s' \n" +
                    "    AND tt.table_schema='%s' " +
                    "    %s\n" +
                    "ORDER BY\n" +
                    "    t.relname,\n" +
                    "    i.relname,\n" +
                    "    a.attnum";
}
