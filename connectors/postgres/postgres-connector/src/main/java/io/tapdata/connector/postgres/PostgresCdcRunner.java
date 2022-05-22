package io.tapdata.connector.postgres;

import io.debezium.embedded.EmbeddedEngine;
import io.debezium.engine.DebeziumEngine;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.connector.postgres.config.PostgresDebeziumConfig;
import io.tapdata.connector.postgres.kit.EmptyKit;
import io.tapdata.connector.postgres.storage.PostgresOffsetStorage;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * CDC runner for Postgresql
 *
 * @author Jarad
 * @date 2022/5/13
 */
public class PostgresCdcRunner extends DebeziumCdcRunner {

    private PostgresJdbcContext postgresJdbcContext;
    private PostgresConfig postgresConfig;
    private PostgresDebeziumConfig postgresDebeziumConfig;
    private List<String> observedTableList;
    private PostgresOffset postgresOffset;
    private int recordSize;
    private StreamReadConsumer consumer;

    public PostgresCdcRunner() {

    }

    public PostgresOffset getPostgresOffset() {
        return postgresOffset;
    }

    public PostgresCdcRunner use(PostgresConfig postgresConfig) {
        this.postgresConfig = postgresConfig;
        this.postgresJdbcContext = new PostgresJdbcContext(postgresConfig);
        return this;
    }

    public PostgresCdcRunner watch(List<String> observedTableList) {
        this.observedTableList = observedTableList;
        postgresDebeziumConfig = new PostgresDebeziumConfig()
                .use(postgresConfig)
                .watch(observedTableList);
        this.runnerName = postgresDebeziumConfig.getSlotName();
        return this;
    }

    public PostgresCdcRunner offset(Object offsetState) {
        if (EmptyKit.isNull(offsetState)) {
            postgresOffset = new PostgresOffset();
        } else {
            this.postgresOffset = (PostgresOffset) offsetState;
        }
        PostgresOffsetStorage.postgresOffsetMap.put(runnerName, postgresOffset);
        return this;
    }

    public PostgresCdcRunner registerConsumer(StreamReadConsumer consumer, int recordSize) {
        this.recordSize = recordSize;
        this.consumer = consumer;
        //build debezium engine
        this.engine = (EmbeddedEngine) EmbeddedEngine.create()
                .using(postgresDebeziumConfig.create())
                .using(new DebeziumEngine.ConnectorCallback() {
                    @Override
                    public void taskStarted() {
                        DebeziumEngine.ConnectorCallback.super.taskStarted();
                        consumer.streamReadStarted();
                    }

                    @Override
                    public void taskStopped() {
                        DebeziumEngine.ConnectorCallback.super.taskStopped();
                        consumer.streamReadEnded();
                    }
                })
//                .using((b, s, throwable) -> {
//
//                })
//                .using(this.getClass().getClassLoader())
//                .using(Clock.SYSTEM)
//                .notifying(this::consumeRecord)
                .using((numberOfMessagesSinceLastCommit, timeSinceLastCommit) -> true)
                .notifying(this::consumeRecords)
                .build();
        //make replica identity for postgres those without unique key
//        try {
//            makeReplicaIdentity();
//        } catch (Throwable e) {
//            e.printStackTrace();
//        }
        return this;
    }

//    public void consumeRecord(SourceRecord sourceRecord) {
//        System.out.println(sourceRecord);
//        System.out.println(TapSimplify.toJson(postgresOffset));
//        System.out.println(recordSize);
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
            switch (op) { //snapshot.mode = 'never'
                case "c": //after running --insert
                case "r": //after slot but before running --read
                    eventList.add(new TapInsertRecordEvent().table(table).after(getMapFromStruct(after)));
                    break;
                case "d": //after running --delete
                    eventList.add(new TapDeleteRecordEvent().table(table).before(getMapFromStruct(before)));
                    break;
                case "u": //after running --update
                    eventList.add(new TapUpdateRecordEvent().table(table).after(getMapFromStruct(after)).before(getMapFromStruct(before)));
                    break;
                default:
                    break;
            }
            if (eventList.size() >= recordSize) {
                consumer.accept(eventList);
                eventList = TapSimplify.list();
            }
        }
        if (EmptyKit.isNotEmpty(eventList)) {
            consumer.accept(eventList);
        }
    }

    private DataMap getMapFromStruct(Struct struct) {
        DataMap dataMap = new DataMap();
        struct.schema().fields().forEach(field -> {
            Object obj = struct.get(field.name());
            if (obj instanceof ByteBuffer) {
                obj = struct.getBytes(field.name());
            }
            dataMap.put(field.name(), obj);
        });
        return dataMap;
    }

    @Override
    public void closeCdcRunner(Object needClearSlot) throws IOException {
        super.closeCdcRunner(needClearSlot);
        if (EmptyKit.isNotNull(needClearSlot) && (boolean) needClearSlot) {
            clearSlot();
        }
        postgresJdbcContext.close();
    }

    // TODO: 2022/5/22 clearSlot
    private void clearSlot() {

    }

    //make these tables ready for REPLICA IDENTITY
    private void makeReplicaIdentity() throws Throwable {
        String tableSql = EmptyKit.isEmpty(observedTableList) ? "" : " AND tab.tablename IN ('" +
                observedTableList.stream().reduce((v1, v2) -> v1 + "','" + v2).orElseGet(String::new) + "')";
        List<String> tableD = TapSimplify.list(); //to make table IDENTITY 'D'
        List<String> tableF = TapSimplify.list(); //to make table IDENTITY 'F'
        postgresJdbcContext.query(String.format(PG_ALL_REPLICA_IDENTITY, postgresConfig.getSchema(), tableSql), resultSet -> {
            while (resultSet.next()) {
                int hasUnique = resultSet.getInt("hasunique");
                String relReplident = resultSet.getString("relreplident");
                String tableName = resultSet.getString("tablename");
                if (hasUnique == 1 && "n,i".contains(relReplident)) {
                    tableD.add(tableName);
                } else if (hasUnique == 0 && !"f".equals(relReplident)) {
                    tableF.add(tableName);
                }
            }
        });
        for (String d : tableD) {
            postgresJdbcContext.execute("ALTER TABLE \"" + d + "\" REPLICA IDENTITY DEFAULT");
        }
        for (String f : tableF) {
            postgresJdbcContext.execute("ALTER TABLE \"" + f + "\" REPLICA IDENTITY FULL");
        }
    }

    private final static String PG_ALL_REPLICA_IDENTITY = "SELECT tab.tablename,cls.relreplident,\n" +
            "    (CASE WHEN EXISTS \n" +
            "        (SELECT 1 FROM pg_index WHERE indrelid IN \n" +
            "            (SELECT oid FROM pg_class WHERE relname = tab.tablename)) \n" +
            "    THEN 1 ELSE 0 END) hasunique \n" +
            "FROM pg_tables tab\n" +
            "JOIN pg_class cls ON cls.relname = tab.tablename\n" +
            "WHERE tab.schemaname = '%s' %s";

}
