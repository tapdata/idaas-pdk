package io.tapdata.postgres;

import io.debezium.embedded.EmbeddedEngine;
import io.debezium.engine.DebeziumEngine;
import io.debezium.util.Clock;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.postgres.config.PostgresConfig;
import io.tapdata.postgres.config.PostgresDebeziumConfig;
import io.tapdata.postgres.kit.SmartKit;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;


public class PostgresCdcRunner extends DebeziumCdcRunner {

    private final PostgresConfig postgresConfig;
    private Object offsetState;
    private int recordSize;
    private StreamReadConsumer consumer;

    public PostgresCdcRunner(PostgresConfig postgresConfig, List<String> observedTableList) {
        this.postgresConfig = postgresConfig;
        postgresDebeziumConfig = new PostgresDebeziumConfig(postgresConfig, observedTableList);
        this.runnerName = postgresDebeziumConfig.getSlotName();
    }

    public PostgresCdcRunner registerConsumer(Object offsetState, int recordSize, StreamReadConsumer consumer) {
        this.engine = EmbeddedEngine.create()
                .using(postgresDebeziumConfig.create())
                .using(this.getClass().getClassLoader())
                .using(Clock.SYSTEM)
//                .notifying(this::consumeRecord)
                .notifying(this::consumeRecords)
                .build();
        // TODO: 2022/5/13 observed tables must have unique key or index, otherwise need to open <REPLICA IDENTITY> 
        this.offsetState = offsetState;
        this.recordSize = recordSize;
        this.consumer = consumer;
        return this;
    }

//    public void consumeRecord(SourceRecord sourceRecord) {
//        System.out.println(sourceRecord);
//        System.out.println(offsetState);
//        System.out.println(recordSize);
//        DebeziumCdcPool.removeRunner(slotName);
//    }

    @Override
    public void consumeRecords(List<SourceRecord> sourceRecords, DebeziumEngine.RecordCommitter<SourceRecord> committer) {
        super.consumeRecords(sourceRecords, committer);
        List<TapEvent> eventList = TapSimplify.list();
        for (SourceRecord sr : sourceRecords) {
            Struct struct = ((Struct) sr.value());
            if (struct == null) {
                return;
            }
            String op = struct.getString("op");
            String table = struct.getStruct("source").getString("table");
            Struct after = struct.getStruct("after");
            Struct before = struct.getStruct("before");
            switch (op) {
                case "c":
                    eventList.add(new TapInsertRecordEvent().table(table).after(getMapFromStruct(after)));
                    break;
                case "r":
                    try {
                        committer.markProcessed(sr);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    break;
                case "d":
                    eventList.add(new TapDeleteRecordEvent().table(table).before(getMapFromStruct(before)));
                    break;
                case "u":
                    eventList.add(new TapUpdateRecordEvent().table(table).after(getMapFromStruct(after)).before(getMapFromStruct(before)));
                    break;
                default:
                    break;
            }
            if (eventList.size() >= recordSize) {
                System.out.println(TapSimplify.toJson(eventList));
//                consumer.accept(eventList);
                eventList = TapSimplify.list();
            }
        }
        if(SmartKit.isNotEmpty(eventList)) {
            System.out.println(TapSimplify.toJson(eventList));
//            consumer.accept(eventList);
        }
//        consumer.streamReadEnded();
    }

    private DataMap getMapFromStruct(Struct struct) {
        DataMap dataMap = new DataMap();
        struct.schema().fields().forEach(field -> {
            dataMap.put(field.name(), struct.get(field.name()));
        });
        return dataMap;
    }

    @Override
    public void releaseResource() {
        try {
            String dbUrl = postgresConfig.getDatabaseUrl();
            Class.forName(postgresConfig.getJdbcDriver());
            Connection conn = DriverManager.getConnection(dbUrl, postgresConfig.getUser(), postgresConfig.getPassword());
            conn.createStatement().execute("SELECT PG_DROP_REPLICATION_SLOT('" + runnerName + "')");
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
