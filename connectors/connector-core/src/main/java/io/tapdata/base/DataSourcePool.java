package io.tapdata.base;

import com.zaxxer.hikari.HikariDataSource;
import io.tapdata.config.CommonDbConfig;
import io.tapdata.kit.EmptyKit;

import java.util.concurrent.ConcurrentHashMap;

public class DataSourcePool {

    private final static ConcurrentHashMap<String, JdbcContext> dataPool = new ConcurrentHashMap<>(16);

    public static JdbcContext getJdbcContext(CommonDbConfig config, Class<? extends JdbcContext> clazz) {
        String key = uniqueKeyForDb(config);
        if (dataPool.containsKey(key)) {
            return dataPool.get(key).incrementAndGet();
        } else {
            JdbcContext context = null;
            try {
                context = clazz.getDeclaredConstructor(config.getClass(), HikariDataSource.class)
                        .newInstance(config, HikariConnection.getHikariDataSource(config));
            } catch (Exception e) {
                context = new JdbcContext(config, HikariConnection.getHikariDataSource(config));
            }
            dataPool.put(key, context);
            return context;
        }
    }

    public static void removeJdbcContext(CommonDbConfig config) {
        dataPool.remove(uniqueKeyForDb(config));
    }

    private static String uniqueKeyForDb(CommonDbConfig config) {
        return config.getHost() + config.getPort() + config.getDatabase() + config.getSchema();
    }

    static class HikariConnection {
        public static HikariDataSource getHikariDataSource(CommonDbConfig config) throws IllegalArgumentException {
            HikariDataSource hikariDataSource;
            if (EmptyKit.isNull(config)) {
                throw new IllegalArgumentException("Config cannot be null");
            }
            hikariDataSource = new HikariDataSource();
            hikariDataSource.setDriverClassName(config.getJdbcDriver());
            hikariDataSource.setJdbcUrl(config.getDatabaseUrl());
            hikariDataSource.setUsername(config.getUser());
            hikariDataSource.setPassword(config.getPassword());
            hikariDataSource.setMinimumIdle(1);
            hikariDataSource.setMaximumPoolSize(20);
            hikariDataSource.setAutoCommit(false);
            hikariDataSource.setIdleTimeout(60 * 1000L);
            hikariDataSource.setKeepaliveTime(60 * 1000L);
            return hikariDataSource;
        }
    }
}
