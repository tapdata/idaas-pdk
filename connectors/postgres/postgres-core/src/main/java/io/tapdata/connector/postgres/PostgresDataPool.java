package io.tapdata.connector.postgres;

import com.zaxxer.hikari.HikariDataSource;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.kit.EmptyKit;

import java.util.concurrent.ConcurrentHashMap;

public class PostgresDataPool {

    private final static ConcurrentHashMap<String, PostgresJdbcContext> dataPool = new ConcurrentHashMap<>(16);

    public static PostgresJdbcContext getJdbcContext(PostgresConfig postgresConfig) {
        String key = postgresConfig.getHost() + postgresConfig.getPort();
        if (dataPool.containsKey(key)) {
            return dataPool.get(key).incrementAndGet();
        } else {
            PostgresJdbcContext context = new PostgresJdbcContext(postgresConfig, HikariConnection.getHikariDataSource(postgresConfig));
            dataPool.put(key, context);
            return context;
        }
    }

    public static void removeJdbcContext(PostgresConfig postgresConfig) {
        String key = postgresConfig.getHost() + postgresConfig.getPort();
        dataPool.remove(key);
    }

    static class HikariConnection {
        public static HikariDataSource getHikariDataSource(PostgresConfig postgresConfig) throws IllegalArgumentException {
            HikariDataSource hikariDataSource;
            if (EmptyKit.isNull(postgresConfig)) {
                throw new IllegalArgumentException("Postgres Config cannot be null");
            }
            hikariDataSource = new HikariDataSource();
            hikariDataSource.setDriverClassName(postgresConfig.getJdbcDriver());
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
