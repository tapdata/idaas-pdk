package io.tapdata.postgres;

import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.postgres.config.PostgresConfig;
import io.tapdata.util.NetUtil;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;

import static io.tapdata.base.ConnectorBase.testItem;

public class PostgresTest {

    private final TapConnectionContext tapConnectionContext;
    private Connection conn;

    public PostgresTest(TapConnectionContext tapConnectionContext) {
        this.tapConnectionContext = tapConnectionContext;
    }

    public TestItem testHostPort() {
        DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
        String host = String.valueOf(connectionConfig.get("host"));
        int port = ((Number) connectionConfig.get("port")).intValue();
        try {
            NetUtil.validateHostPortWithSocket(host, port);
            return testItem(PostgresTestItem.HOST_PORT.getContent(), TestItem.RESULT_SUCCESSFULLY);
        } catch (IOException e) {
            return testItem(PostgresTestItem.HOST_PORT.getContent(), TestItem.RESULT_FAILED, e.getMessage());
        }
    }

    public TestItem testConnect() {
        DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
        try {
            Class.forName("org.postgresql.Driver");
            PostgresConfig postgresConfig = PostgresConfig.load(connectionConfig);
            conn = DriverManager.getConnection(postgresConfig.getDatabaseUrl(), postgresConfig.getUser(), postgresConfig.getPassword());
            return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY);
        } catch (Exception e) {
            return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, e.getMessage());
        }
    }

    public TestItem testPrivilege() {
        return null;
    }

    public TestItem testCdc() {
        return null;
    }
}

enum PostgresTestItem {

    HOST_PORT("Check host port is invalid"),
    CHECK_VERSION("Check database version"),
    CHECK_CDC_PRIVILEGES("Check database cdc privileges"),
    CHECK_CREATE_TABLE_PRIVILEGE("Check create table privilege"),
    ;

    private final String content;

    PostgresTestItem(String content) {
        this.content = content;
    }

    public String getContent() {
        return content;
    }
}
