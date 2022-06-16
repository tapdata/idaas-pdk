package io.tapdata.connector.postgres;

import io.tapdata.common.DataSourcePool;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.util.NetUtil;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.tapdata.base.ConnectorBase.testItem;

// TODO: 2022/6/9 need to improve test items 
public class PostgresTest implements AutoCloseable {

    private final PostgresConfig postgresConfig;
    private final PostgresJdbcContext postgresJdbcContext;
    private final String uuid = UUID.randomUUID().toString();

    public PostgresTest(PostgresConfig postgresConfig) {
        this.postgresConfig = postgresConfig;
        postgresJdbcContext = (PostgresJdbcContext) DataSourcePool.getJdbcContext(postgresConfig, PostgresJdbcContext.class, uuid);
    }

    //Test host and port
    public TestItem testHostPort() {
        try {
            NetUtil.validateHostPortWithSocket(postgresConfig.getHost(), postgresConfig.getPort());
            return testItem(PostgresTestItem.HOST_PORT.getContent(), TestItem.RESULT_SUCCESSFULLY);
        } catch (IOException e) {
            return testItem(PostgresTestItem.HOST_PORT.getContent(), TestItem.RESULT_FAILED, e.getMessage());
        }
    }

    //Test pg connect and log in
    public TestItem testConnect() {
        try (
                Connection connection = postgresJdbcContext.getConnection()
        ) {
            return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY);
        } catch (Exception e) {
            return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, e.getMessage());
        }
    }

    //Test number of tables and privileges
    public TestItem testPrivilege() {
        try (
                Connection conn = postgresJdbcContext.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT count(*) FROM information_schema.table_privileges " +
                                "WHERE grantee=? AND table_catalog=? AND table_schema=? ")
        ) {
            AtomicInteger tablePrivileges = new AtomicInteger();
            ps.setObject(1, postgresConfig.getUser());
            ps.setObject(2, postgresConfig.getDatabase());
            ps.setObject(3, postgresConfig.getSchema());
            postgresJdbcContext.query(ps, resultSet -> tablePrivileges.set(resultSet.getInt(1)));
            if (tablePrivileges.get() >= 7 * tableCount()) {
                return testItem(PostgresTestItem.CHECK_TABLE_PRIVILEGE.getContent(), TestItem.RESULT_SUCCESSFULLY);
            } else {
                return testItem(PostgresTestItem.CHECK_TABLE_PRIVILEGE.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                        "Current user may have no all privileges for some tables, Check it!");
            }
        } catch (Throwable e) {
            return testItem(PostgresTestItem.CHECK_TABLE_PRIVILEGE.getContent(), TestItem.RESULT_FAILED, e.getMessage());
        }
    }

    public TestItem testReplication() {
        try {
            AtomicBoolean rolReplication = new AtomicBoolean();
            postgresJdbcContext.query(String.format(PG_ROLE_INFO, postgresConfig.getUser()),
                    resultSet -> rolReplication.set(resultSet.getBoolean("rolreplication")));
            if (rolReplication.get()) {
                return testItem(PostgresTestItem.CHECK_CDC_PRIVILEGES.getContent(), TestItem.RESULT_SUCCESSFULLY);
            } else {
                return testItem(PostgresTestItem.CHECK_CDC_PRIVILEGES.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                        "Current user have no privileges to create Replication Slot!");
            }
        } catch (Throwable e) {
            return testItem(PostgresTestItem.CHECK_CDC_PRIVILEGES.getContent(), TestItem.RESULT_FAILED, e.getMessage());
        }
    }

    public TestItem testLogPlugin() {
        try {
            List<String> testSqls = TapSimplify.list();
            testSqls.add(String.format(PG_LOG_PLUGIN_CREATE_TEST, postgresConfig.getLogPluginName()));
            testSqls.add(PG_LOG_PLUGIN_DROP_TEST);
            postgresJdbcContext.batchExecute(testSqls);
            return testItem(PostgresTestItem.CHECK_LOG_PLUGIN.getContent(), TestItem.RESULT_SUCCESSFULLY);
        } catch (Throwable e) {
            return testItem(PostgresTestItem.CHECK_LOG_PLUGIN.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                    "Invalid log plugin, Maybe cdc events cannot work!");
        }
    }

    private int tableCount() throws Throwable {
        AtomicInteger tableCount = new AtomicInteger();
        postgresJdbcContext.query(PG_TABLE_NUM, resultSet -> tableCount.set(resultSet.getInt(1)));
        return tableCount.get();
    }

    @Override
    public void close() {
        try {
            postgresJdbcContext.finish(uuid);
        } catch (Exception ignored) {
        }
    }

    private final static String PG_ROLE_INFO = "SELECT * FROM pg_roles WHERE rolname='%s'";
    private final static String PG_TABLE_NUM = "SELECT COUNT(*) FROM pg_tables WHERE schemaname='%s'";
    private final static String PG_LOG_PLUGIN_CREATE_TEST = "SELECT pg_create_logical_replication_slot('pg_slot_test','%s')";
    private final static String PG_LOG_PLUGIN_DROP_TEST = "SELECT pg_drop_replication_slot('pg_slot_test')";
}

enum PostgresTestItem {

    HOST_PORT("Check host port is invalid"),
    //    CHECK_VERSION("Check database version"),
    CHECK_CDC_PRIVILEGES("Check replication privileges"),
    CHECK_TABLE_PRIVILEGE("Check all for table privilege"),
    CHECK_LOG_PLUGIN("Check log plugin for database"),
    ;

    private final String content;

    PostgresTestItem(String content) {
        this.content = content;
    }

    public String getContent() {
        return content;
    }
}
