package io.tapdata.postgres.config;

import io.debezium.config.Configuration;
import io.tapdata.postgres.kit.SmartKit;

import java.util.List;

/**
 * debezium config for postgres
 *
 * @author Jarad
 * @date 2022/5/10
 */
public class DebeziumConfig {

    private final PostgresConfig postgresConfig;
    private List<String> observedTableList;

    public DebeziumConfig(PostgresConfig postgresConfig) {
        this.postgresConfig = postgresConfig;
    }

    public List<String> getObservedTableList() {
        return observedTableList;
    }

    public void setObservedTableList(List<String> observedTableList) {
        this.observedTableList = observedTableList;
    }

    public Configuration create() {
        Configuration.Builder builder = Configuration.create();
        builder.with("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
                .with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
                .with("offset.storage.file.filename", "/path/cdc/offset/Student.dat")
                .with("offset.flush.interval.ms", 60000)
                .with("name", postgresConfig.getDatabase() + "-postgres-connector")
                .with("database.server.name", postgresConfig.getHost() + "-" + postgresConfig.getDatabase())
                .with("database.hostname", postgresConfig.getHost())
                .with("database.port", postgresConfig.getPort())
                .with("database.user", postgresConfig.getUser())
                .with("database.password", postgresConfig.getPassword())
                .with("database.dbname", postgresConfig.getDatabase());
        if (SmartKit.isNotEmpty(observedTableList)) {
            String tableWhiteList = observedTableList.stream().map(v -> postgresConfig.getSchema() + "." + v).reduce((v1, v2) -> v1 + "," + v2).orElseGet(String::new);
            builder.with("table.whitelist", tableWhiteList);
        }
        return builder.build();
    }

}
