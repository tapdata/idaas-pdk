import io.tapdata.connector.postgres.PostgresCdcRunner;
import io.tapdata.connector.postgres.PostgresOffset;
import io.tapdata.connector.postgres.config.PostgresConfig;

import java.util.Collections;

public class Main2 {
    public static void main(String[] args) throws Throwable {
        PostgresConfig postgresConfig = new PostgresConfig();
        postgresConfig.setHost("192.168.1.189");
        postgresConfig.setPort(5432);
        postgresConfig.setDatabase("COOLGJ");
        postgresConfig.setSchema("public");
        postgresConfig.setExtParams("");
        postgresConfig.setUser("postgres");
        postgresConfig.setPassword("gj0628");
        PostgresOffset postgresOffset = new PostgresOffset();
        postgresOffset.setStreamOffsetKey("{\"schema\":null,\"payload\":[\"cdc_77091c94_0d73_3d1d_b4e2_ed6effc535d3-postgres-connector\",{\"server\":\"COOLGJ\"}]}");
        postgresOffset.setStreamOffsetValue("{\"transaction_id\":null,\"lsn_proc\":181856448,\"lsn_commit\":181856448,\"lsn\":181856448,\"txId\":7266,\"ts_usec\":1553158676829305}");
        PostgresCdcRunner cdcRunner = new PostgresCdcRunner()
                .use(postgresConfig)
                .watch(Collections.singletonList("Student"))
                .offset(postgresOffset)
                .registerConsumer(null, 10);
        new Thread(cdcRunner::startCdcRunner).start();
        Thread.sleep(20000);

        cdcRunner.closeCdcRunner(null);
    }
}
