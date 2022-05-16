package io.tapdata.postgres;

import io.debezium.embedded.EmbeddedEngine;
import io.debezium.engine.DebeziumEngine;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.postgres.config.PostgresConfig;
import io.tapdata.postgres.config.PostgresDebeziumConfig;
import io.tapdata.postgres.kit.SmartKit;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.nio.ByteBuffer;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * CDC runner for Postgresql
 *
 * @author Jarad
 * @date 2022/5/13
 */
public class PostgresCdcRunner extends DebeziumCdcRunner {

    private Statement stmt;
    private Connection conn;
    private final PostgresConfig postgresConfig;
    private final PostgresDebeziumConfig postgresDebeziumConfig;
    private final List<TapTable> observedTableList;
    private HashMap<String, String> replicaMarks;
    private Object offsetState;
    private int recordSize;
    private StreamReadConsumer consumer;

    public PostgresCdcRunner(PostgresConfig postgresConfig, List<TapTable> observedTableList) {
        //initial postgres db connection
        this.postgresConfig = postgresConfig;
        try {
            String dbUrl = postgresConfig.getDatabaseUrl();
            Class.forName(postgresConfig.getJdbcDriver());
            conn = DriverManager.getConnection(dbUrl, postgresConfig.getUser(), postgresConfig.getPassword());
            stmt = conn.createStatement();
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        //initial debezium config for postgres
        this.observedTableList = observedTableList;
        postgresDebeziumConfig = new PostgresDebeziumConfig(postgresConfig, SmartKit.isNotEmpty(observedTableList) ?
                observedTableList.stream().map(TapTable::getId).collect(Collectors.toList()) : null);
        this.runnerName = postgresDebeziumConfig.getSlotName();
    }

    public PostgresCdcRunner registerConsumer(Object offsetState, int recordSize, StreamReadConsumer consumer) {
        //build debezium engine
        this.engine = EmbeddedEngine.create()
                .using(postgresDebeziumConfig.create())
//                .using(this.getClass().getClassLoader())
//                .using(Clock.SYSTEM)
//                .notifying(this::consumeRecord)
                .notifying(this::consumeRecords)
                .build();
        this.offsetState = offsetState;
        this.recordSize = recordSize;
        this.consumer = consumer;
//        try {
//            makeReplicaMarks();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
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
                System.out.println(TapSimplify.toJson(eventList));
                consumer.accept(eventList);
                eventList = TapSimplify.list();
            }
        }
        if (SmartKit.isNotEmpty(eventList)) {
            System.out.println(TapSimplify.toJson(eventList));
            consumer.accept(eventList);
        }
//        consumer.streamReadEnded();
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
    public void releaseResource() {
        try {
            stmt.execute("SELECT PG_DROP_REPLICATION_SLOT('" + runnerName + "')");
            stmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //make these tables ready for REPLICA IDENTITY
    private void makeReplicaMarks() throws SQLException {
        String tableSql = SmartKit.isEmpty(observedTableList) ? "" : " AND tab.tablename IN (" +
                observedTableList.stream().map(TapTable::getId).reduce((v1, v2) -> "'" + v1 + "','" + v2 + "'").orElseGet(String::new) + ")";
        ResultSet resultSet = stmt.executeQuery("SELECT " +
                "tab.tablename,cls.relreplident," +
                "(CASE WHEN EXISTS (SELECT 1 FROM pg_index WHERE indrelid IN " +
                "(SELECT oid FROM pg_class WHERE relname = tab.tablename)) THEN 1 ELSE 0 END) hasunique " +
                "FROM pg_tables tab \n" +
                "JOIN pg_class cls ON cls.relname = tab.tablename\n" +
                "WHERE tab.schemaname = 'public'" + tableSql);
        List<String> tableD = TapSimplify.list(); //to make table IDENTITY 'D'
        List<String> tableF = TapSimplify.list(); //to make table IDENTITY 'F'
        while (resultSet.next()) {
            int hasUnique = resultSet.getInt("hasunique");
            String relReplident = resultSet.getString("relreplident");
            String tableName = resultSet.getString("tablename");
            if (hasUnique == 1 && "n,i".contains(relReplident)) {
                tableD.add(tableName);
            }
            else if (hasUnique == 0 && !"f".equals(relReplident)) {
                tableF.add(tableName);
            }
        }
    }

}
