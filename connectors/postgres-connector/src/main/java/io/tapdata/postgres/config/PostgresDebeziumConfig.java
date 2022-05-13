package io.tapdata.postgres.config;

import io.debezium.config.Configuration;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.postgres.kit.SmartKit;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * debezium config for postgres
 *
 * @author Jarad
 * @date 2022/5/10
 */
public class PostgresDebeziumConfig {

    private final PostgresConfig postgresConfig;
    private final List<String> observedTableList;
    private final String slotName; //unique for each slot, so create it by postgres config and observed tables

    public PostgresDebeziumConfig(PostgresConfig postgresConfig, List<String> observedTableList) {
        this.postgresConfig = postgresConfig;
        this.observedTableList = observedTableList;
        //unique and can find it
        this.slotName = "cdc_" + UUID.nameUUIDFromBytes((TapSimplify.toJson(postgresConfig)
                        + TapSimplify.toJson(observedTableList.stream().sorted().collect(Collectors.toList()))).getBytes())
                .toString().replaceAll("-", "_");
    }

    public List<String> getObservedTableList() {
        return observedTableList;
    }

    public String getSlotName() {
        return slotName;
    }

    /**
     * create debezium config
     *
     * @return Configuration
     */
    public Configuration create() {
        Configuration.Builder builder = Configuration.create();
        builder.with("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
                .with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
                .with("slot.name", slotName)
                .with("offset.storage.file.filename", "d:/cdc/offset/" + slotName + ".dat") //path must be changed with requirement
                .with("offset.flush.interval.ms", 60000)
                .with("name", slotName + "-postgres-connector")
                .with("database.server.name", postgresConfig.getHost() + "-" + postgresConfig.getDatabase())
                .with("database.hostname", postgresConfig.getHost())
                .with("database.port", postgresConfig.getPort())
                .with("database.user", postgresConfig.getUser())
                .with("database.password", postgresConfig.getPassword())
                .with("database.dbname", postgresConfig.getDatabase());
        if (SmartKit.isNotEmpty(observedTableList)) {
            //construct tableWhiteList with schema.table(,) as <public.Student,postgres.test>
            String tableWhiteList = observedTableList.stream().map(v -> postgresConfig.getSchema() + "." + v).reduce((v1, v2) -> v1 + "," + v2).orElseGet(String::new);
            builder.with("table.whitelist", tableWhiteList);
        }
        return builder.build();
    }

}
