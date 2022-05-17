package io.tapdata.postgres;

import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.postgres.config.PostgresConfig;
import io.tapdata.util.NetUtil;

import java.io.IOException;
import java.sql.*;

import static io.tapdata.base.ConnectorBase.testItem;

public class PostgresTest {

    private final PostgresConfig postgresConfig;
    private Connection conn;

    public PostgresTest(TapConnectionContext tapConnectionContext) {
        DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
        try {
            postgresConfig = PostgresConfig.load(connectionConfig);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(postgresConfig.getDatabaseUrl(), postgresConfig.getUser(), postgresConfig.getPassword());
            return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY);
        } catch (Exception e) {
            return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, e.getMessage());
        }
    }

    //Test number of tables and privileges
    public TestItem testPrivilege() {
        try {
            PreparedStatement ps = conn.prepareStatement(
                    "SELECT * FROM information_schema.table_privileges " +
                            "WHERE grantee=? AND table_catalog=? AND table_schema=? ");
            ps.setObject(1, postgresConfig.getUser());
            ps.setObject(2, postgresConfig.getDatabase());
            ps.setObject(3, postgresConfig.getSchema());
            ResultSet privilegeResult = ps.executeQuery();
            privilegeResult.last();
            if (privilegeResult.getRow() >= 7 * tableCount()) {
                return testItem(PostgresTestItem.CHECK_TABLE_PRIVILEGE.getContent(), TestItem.RESULT_SUCCESSFULLY);
            } else {
                return testItem(PostgresTestItem.CHECK_TABLE_PRIVILEGE.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                        "Current user may have no all privileges for some tables, Check it!");
            }
        } catch (SQLException e) {
            return testItem(PostgresTestItem.CHECK_TABLE_PRIVILEGE.getContent(), TestItem.RESULT_FAILED, e.getMessage());
        }
    }

    public TestItem testReplication() {
        try {
            ResultSet resultSet = conn.createStatement().executeQuery(
                    "SELECT * FROM  pg_roles WHERE rolname='" + postgresConfig.getUser() + "'");
            resultSet.last();
            if (resultSet.getBoolean("rolreplication")) {
                return testItem(PostgresTestItem.CHECK_CDC_PRIVILEGES.getContent(), TestItem.RESULT_SUCCESSFULLY);
            } else {
                return testItem(PostgresTestItem.CHECK_CDC_PRIVILEGES.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                        "Current user have no privileges to create Replication Slot!");
            }
        } catch (SQLException e) {
            return testItem(PostgresTestItem.CHECK_CDC_PRIVILEGES.getContent(), TestItem.RESULT_FAILED, e.getMessage());
        }
    }

    private int tableCount() throws SQLException {
        DatabaseMetaData databaseMetaData = conn.getMetaData();
        ResultSet tableResult = databaseMetaData.getTables(conn.getCatalog(), postgresConfig.getSchema(), null, new String[]{"TABLE"});
        tableResult.last();
        return tableResult.getRow();
    }
}

enum PostgresTestItem {

    HOST_PORT("Check host port is invalid"),
    //    CHECK_VERSION("Check database version"),
    CHECK_CDC_PRIVILEGES("Check replication privileges"),
    CHECK_TABLE_PRIVILEGE("Check all for table privilege"),
    ;

    private final String content;

    PostgresTestItem(String content) {
        this.content = content;
    }

    public String getContent() {
        return content;
    }
}
