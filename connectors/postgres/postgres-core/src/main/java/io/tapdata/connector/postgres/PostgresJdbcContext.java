package io.tapdata.connector.postgres;

import com.zaxxer.hikari.HikariDataSource;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.sql.*;

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
            if (connection != null && connection.isValid(5) && !connection.getAutoCommit()) {
                connection.commit();
            }
        } catch (Throwable ignored) {
        }
    }

    public static void tryRollBack(Connection connection) {
        try {
            if (connection != null && connection.isValid(5) && !connection.getAutoCommit()) {
                connection.rollback();
            }
        } catch (Throwable ignored) {
        }
    }

    public void query(String sql, ResultSetConsumer resultSetConsumer) throws Throwable {
        TapLogger.debug(TAG, "Execute query, sql: " + sql);
        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)
        ) {
            if (null != resultSet) {
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
            if (null != resultSet) {
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
            statement.execute(sql);
        } catch (SQLException e) {
            throw new Exception("Execute sql failed, sql: " + sql + ", message: " + e.getSQLState() + " " + e.getErrorCode() + " " + e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        if (hikariDataSource != null) {
            hikariDataSource.close();
        }
    }

    static class HikariConnection {
        public static HikariDataSource getHikariDataSource(PostgresConfig postgresConfig) throws IllegalArgumentException {
            HikariDataSource hikariDataSource;
            if (null == postgresConfig) {
                throw new IllegalArgumentException("Postgres Config cannot be null");
            }
            hikariDataSource = new HikariDataSource();
            hikariDataSource.setDriverClassName("org.postgresql.Driver");
            hikariDataSource.setJdbcUrl(postgresConfig.getDatabaseUrl());
            hikariDataSource.setUsername(postgresConfig.getUser());
            hikariDataSource.setPassword(postgresConfig.getPassword());
            hikariDataSource.setMinimumIdle(1);
            hikariDataSource.setMaximumPoolSize(20);
            hikariDataSource.setAutoCommit(false);
            hikariDataSource.setIdleTimeout(60 * 1000L);
            hikariDataSource.setKeepaliveTime(60 * 1000L);
            return hikariDataSource;
        }
    }
}
