package io.tapdata.common;

import com.zaxxer.hikari.HikariDataSource;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.kit.EmptyKit;

import java.sql.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public abstract class JdbcContext {

    private final static String TAG = JdbcContext.class.getSimpleName();
    private final HikariDataSource hikariDataSource;
    private final CommonDbConfig config;
    private final AtomicInteger connectorNumber = new AtomicInteger(1);

    public JdbcContext incrementAndGet() {
        connectorNumber.incrementAndGet();
        return this;
    }

    public JdbcContext(CommonDbConfig config, HikariDataSource hikariDataSource) {
        this.config = config;
        this.hikariDataSource = hikariDataSource;
    }

    public CommonDbConfig getConfig() {
        return config;
    }

    public Connection getConnection() throws SQLException {
        return hikariDataSource.getConnection();
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
            DataSourcePool.removeJdbcContext(config);
        }
    }
}
