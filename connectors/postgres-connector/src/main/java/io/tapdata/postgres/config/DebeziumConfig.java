package io.tapdata.postgres.config;

import io.debezium.config.Configuration;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.postgres.kit.SmartKit;

import java.util.List;
import java.util.UUID;

/**
 * debezium config for postgres
 *
 * @author Jarad
 * @date 2022/5/10
 */
public class DebeziumConfig {

    private final PostgresConfig postgresConfig;
    private final List<String> observedTableList;
    private final String slotName;
    private final String hostPort;

    public DebeziumConfig(PostgresConfig postgresConfig, List<String> observedTableList) {
        this.postgresConfig = postgresConfig;
        this.observedTableList = observedTableList;
        this.hostPort = postgresConfig.getHost() + ":" + postgresConfig.getPort();
//        this.slotName = "cdc_" + UUID.randomUUID().toString().replaceAll("-", "_");
        this.slotName = "cdc_" + UUID.nameUUIDFromBytes((TapSimplify.toJson(postgresConfig)
                + TapSimplify.toJson(observedTableList)).getBytes()).toString().replaceAll("-", "_");
        System.out.println(slotName);
    }

    public List<String> getObservedTableList() {
        return observedTableList;
    }

    public String getSlotName() {
        return slotName;
    }

    public String getHostPort() {
        return hostPort;
    }

    public Configuration create() {
        Configuration.Builder builder = Configuration.create();
        builder.with("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
                .with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
                .with("slot.name", slotName)
                .with("offset.storage.file.filename", "d:/cdc/offset/" + slotName + ".dat")
                .with("offset.flush.interval.ms", 60000)
                .with("name", slotName + "-postgres-connector")
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
