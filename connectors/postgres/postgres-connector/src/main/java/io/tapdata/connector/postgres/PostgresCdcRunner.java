package io.tapdata.connector.postgres;

import io.debezium.embedded.EmbeddedEngine;
import io.debezium.engine.DebeziumEngine;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.connector.postgres.config.PostgresDebeziumConfig;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.NumberKit;
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
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    public PostgresCdcRunner useSlot(String slotName) {
        this.runnerName = slotName;
        return this;
    }

    public PostgresCdcRunner watch(List<String> observedTableList) {
        this.observedTableList = observedTableList;
        postgresDebeziumConfig = new PostgresDebeziumConfig()
                .use(postgresConfig)
                .watch(observedTableList);
        if (EmptyKit.isNotNull(runnerName)) {
            postgresDebeziumConfig.useSlot(runnerName);
        }
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
//                .using((numberOfMessagesSinceLastCommit, timeSinceLastCommit) ->
//                        numberOfMessagesSinceLastCommit >= 1000 || timeSinceLastCommit.getSeconds() >= 60)
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

    public void consumeRecord(SourceRecord sourceRecord) {
        System.out.println(sourceRecord);
    }

    @Override
    public void consumeRecords(List<SourceRecord> sourceRecords, DebeziumEngine.RecordCommitter<SourceRecord> committer) {
        super.consumeRecords(sourceRecords, committer);
        List<TapEvent> eventList = TapSimplify.list();
        Map<String, ?> offset = null;
        for (SourceRecord sr : sourceRecords) {
            offset = sr.sourceOffset();
            Long referenceTime = (Long) offset.get("ts_usec");
            Struct struct = ((Struct) sr.value());
            if (struct == null) {
                continue;
            }
            String op = struct.getString("op");
            String table = struct.getStruct("source").getString("table");
            Struct after = struct.getStruct("after");
            Struct before = struct.getStruct("before");
            switch (op) { //snapshot.mode = 'never'
                case "c": //after running --insert
                case "r": //after slot but before running --read
                    eventList.add(new TapInsertRecordEvent().table(table).after(getMapFromStruct(after)).referenceTime(referenceTime));
                    break;
                case "d": //after running --delete
                    eventList.add(new TapDeleteRecordEvent().table(table).before(getMapFromStruct(before)).referenceTime(referenceTime));
                    break;
                case "u": //after running --update
                    eventList.add(new TapUpdateRecordEvent().table(table).after(getMapFromStruct(after)).before(getMapFromStruct(before)).referenceTime(referenceTime));
                    break;
                default:
                    break;
            }
            if (eventList.size() >= recordSize) {
                postgresOffset.setSourceOffset(TapSimplify.toJson(offset));
                consumer.accept(eventList, postgresOffset);
                PostgresOffsetStorage.postgresOffsetMap.put(runnerName, postgresOffset);
                eventList.clear();
            }
        }
        if (EmptyKit.isNotEmpty(eventList)) {
            postgresOffset.setSourceOffset(TapSimplify.toJson(offset));
            consumer.accept(eventList, postgresOffset);
            PostgresOffsetStorage.postgresOffsetMap.put(runnerName, postgresOffset);
        }
    }

    private DataMap getMapFromStruct(Struct struct) {
        DataMap dataMap = new DataMap();
        if (EmptyKit.isNull(struct)) {
            return dataMap;
        }
        struct.schema().fields().forEach(field -> {
            Object obj = struct.get(field.name());
            if (obj instanceof ByteBuffer) {
                obj = struct.getBytes(field.name());
            } else if (obj instanceof Struct) {
                String str = new String(((Struct) obj).getBytes("value"));
                obj = BigDecimal.valueOf(NumberKit.bytes2long(((Struct) obj).getBytes("value")), (int) ((Struct) obj).get("scale"));
            }
            dataMap.put(field.name(), obj);
        });
        return dataMap;
    }

    @Override
    public void closeCdcRunner(Object needClearSlot) throws IOException, SQLException {
        super.closeCdcRunner(needClearSlot);
        if (EmptyKit.isNotNull(needClearSlot) && (boolean) needClearSlot) {
            clearSlot();
        }
        postgresJdbcContext.close();
    }

    private void clearSlot() throws SQLException {
        postgresJdbcContext.execute("SELECT pg_drop_replication_slot('" + runnerName + "')");
    }

    //make these tables ready for REPLICA IDENTITY
    private void makeReplicaIdentity() throws Throwable {
        String tableSql = EmptyKit.isEmpty(observedTableList) ? "" : " AND tab.tablename IN ('" +
                String.join("','", observedTableList) + "')";
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
        List<String> sqls = TapSimplify.list();
        sqls.addAll(tableD.stream().map(d -> "ALTER TABLE \"" + d + "\" REPLICA IDENTITY DEFAULT").collect(Collectors.toList()));
        sqls.addAll(tableF.stream().map(f -> "ALTER TABLE \"" + f + "\" REPLICA IDENTITY FULL").collect(Collectors.toList()));
        postgresJdbcContext.batchExecute(sqls);
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
