package io.tapdata.connector.postgres;

import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PostgresWriteRecorder {

    private final Connection connection;
    private final TapTable tapTable;
    private List<String> uniqueCondition;
    private boolean hasPk = false;
    private PreparedStatement preparedStatement = null;
    private AtomicLong atomicLong = new AtomicLong(0);
    private List<TapRecordEvent> batchCache = TapSimplify.list();
    private String postgresVersion = "PostgreSQL 9.6";

    public PostgresWriteRecorder(Connection connection, TapTable tapTable) {
        this.connection = connection;
        this.tapTable = tapTable;
        analyzeTable();
    }

    private void analyzeTable() {
        if (EmptyKit.isEmpty(tapTable.getIndexList())) {
            uniqueCondition = TapSimplify.list();
        } else if (tapTable.getIndexList().stream().anyMatch(TapIndex::isPrimary)) {
            hasPk = true;
            uniqueCondition = tapTable.getIndexList().stream().filter(TapIndex::isPrimary)
                    .findFirst().orElseGet(TapIndex::new).getIndexFields().stream().map(TapIndexField::getName).collect(Collectors.toList());
        } else if (tapTable.getIndexList().stream().anyMatch(TapIndex::isUnique)) {
            uniqueCondition = tapTable.getIndexList().stream().filter(TapIndex::isUnique)
                    .findFirst().orElseGet(TapIndex::new).getIndexFields().stream().map(TapIndexField::getName).collect(Collectors.toList());
        } else {
            uniqueCondition = TapSimplify.list();
        }
    }

    public void executeBatch(WriteListResult<TapRecordEvent> listResult) {
        long succeed = batchCache.size();
        if (succeed <= 0) {
            return;
        }
        try {
            if (preparedStatement != null) {
                preparedStatement.executeBatch();
                preparedStatement.clearBatch();
                batchCache.clear();
            }
        } catch (SQLException e) {
            Map<TapRecordEvent, Throwable> map = batchCache.stream().collect(Collectors.toMap(Function.identity(), (v) -> e));
            listResult.addErrors(map);
            succeed = 0;
            e.printStackTrace();
        }
        atomicLong.addAndGet(succeed);
    }

    public void addInsertBatch(Map<String, Object> after) throws SQLException {
        if (EmptyKit.isEmpty(after)) {
            return;
        }
        if (EmptyKit.isNull(preparedStatement)) {
            String insertHead = "INSERT INTO \"" + tapTable.getId() + "\" ("
                    + after.keySet().stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", ")) + ") ";
            String insertValue = "VALUES(" + StringKit.copyString("?", after.size(), ",") + ") ";
            String insertSql = insertHead + insertValue;
            if (EmptyKit.isNotEmpty(uniqueCondition)) {
                if (postgresVersion.compareTo("PostgreSQL 9.5") > 0) {
                    insertSql += "ON CONFLICT("
                            + uniqueCondition.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", "))
                            + ") DO UPDATE SET " + after.keySet().stream().map(k -> "\"" + k + "\"=?").collect(Collectors.joining(", "));
                } else {
                    if (hasPk) {
                        insertSql = "WITH upsert AS (UPDATE \"" + tapTable.getId() + "\" SET " + after.keySet().stream().map(k -> "\"" + k + "\"=?")
                                .collect(Collectors.joining(", ")) + " WHERE " + uniqueCondition.stream().map(k -> "\"" + k + "\"=?")
                                .collect(Collectors.joining(" AND ")) + " RETURNING *) " + insertHead + " SELECT "
                                + StringKit.copyString("?", after.size(), ",") + " WHERE NOT EXISTS (SELECT * FROM upsert)";
                    } else {
                        insertSql = "WITH upsert AS (UPDATE \"" + tapTable.getId() + "\" SET " + after.keySet().stream().map(k -> "\"" + k + "\"=?")
                                .collect(Collectors.joining(", ")) + " WHERE " + uniqueCondition.stream().map(k -> "(\"" + k + "\"=? OR (\"" + k + "\" IS NULL AND ?::text IS NULL))")
                                .collect(Collectors.joining(" AND ")) + " RETURNING *) " + insertHead + " SELECT "
                                + StringKit.copyString("?", after.size(), ",") + " WHERE NOT EXISTS (SELECT * FROM upsert)";
                    }
                }
            }
            preparedStatement = connection.prepareStatement(insertSql);
        }
        preparedStatement.clearParameters();
        int pos = 1;
        if (postgresVersion.compareTo("PostgreSQL 9.5") <= 0 && EmptyKit.isNotEmpty(uniqueCondition)) {
            for (String key : after.keySet()) {
                preparedStatement.setObject(pos++, after.get(key));
            }
            for (String key : uniqueCondition) {
                preparedStatement.setObject(pos++, after.get(key));
            }
        }
        for (String key : after.keySet()) {
            preparedStatement.setObject(pos++, after.get(key));
        }
        if (EmptyKit.isNotEmpty(uniqueCondition) && postgresVersion.compareTo("PostgreSQL 9.5") > 0) {
            for (String key : after.keySet()) {
                preparedStatement.setObject(pos++, after.get(key));
            }
        }
        preparedStatement.addBatch();
    }

    //before is always empty
    public void addUpdateBatch(Map<String, Object> before, Map<String, Object> after) throws SQLException {
        if (EmptyKit.isEmpty(after) || EmptyKit.isEmpty(uniqueCondition)) {
            return;
        }
        for (Map.Entry<String, Object> entry : before.entrySet()) {
            after.remove(entry.getKey(), entry.getValue());
        }
        if (EmptyKit.isEmpty(before)) {
            uniqueCondition.forEach(k -> before.put(k, after.get(k)));
        }
        if (EmptyKit.isNull(preparedStatement)) {
            if (hasPk) {
                preparedStatement = connection.prepareStatement("UPDATE \"" + tapTable.getId() + "\" SET " +
                        after.keySet().stream().map(k -> "\"" + k + "\"=?").collect(Collectors.joining(", ")) + " WHERE " +
                        before.keySet().stream().map(k -> "\"" + k + "\"=?").collect(Collectors.joining(" AND ")));
            } else {
                preparedStatement = connection.prepareStatement("UPDATE \"" + tapTable.getId() + "\" SET " +
                        after.keySet().stream().map(k -> "\"" + k + "\"=?").collect(Collectors.joining(", ")) + " WHERE " +
                        before.keySet().stream().map(k -> "(\"" + k + "\"=? OR (\"" + k + "\" IS NULL AND ?::text IS NULL))")
                                .collect(Collectors.joining(" AND ")));
            }
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : after.keySet()) {
            preparedStatement.setObject(pos++, after.get(key));
        }
        dealNullBefore(before, pos);
        preparedStatement.addBatch();
    }

    public void addDeleteBatch(Map<String, Object> before) throws SQLException {
        if (EmptyKit.isEmpty(before)) {
            return;
        }
        if (EmptyKit.isNotEmpty(uniqueCondition)) {
            before.keySet().removeIf(k -> !uniqueCondition.contains(k));
        }
        if (EmptyKit.isNull(preparedStatement)) {
            if (hasPk) {
                preparedStatement = connection.prepareStatement("DELETE FROM \"" + tapTable.getId() + "\" WHERE " +
                        before.keySet().stream().map(k -> "\"" + k + "\"=?").collect(Collectors.joining(" AND ")));
            } else {
                preparedStatement = connection.prepareStatement("DELETE FROM \"" + tapTable.getId() + "\" WHERE " +
                        before.keySet().stream().map(k -> "(\"" + k + "\"=? OR (\"" + k + "\" IS NULL AND ?::text IS NULL))")
                                .collect(Collectors.joining(" AND ")));
            }
        }
        preparedStatement.clearParameters();
        dealNullBefore(before, 1);
        preparedStatement.addBatch();
    }

    private void dealNullBefore(Map<String, Object> before, int pos) throws SQLException {
        if (hasPk) {
            for (String key : before.keySet()) {
                preparedStatement.setObject(pos++, before.get(key));
            }
        } else {
            for (String key : before.keySet()) {
                preparedStatement.setObject(pos++, before.get(key));
                preparedStatement.setObject(pos++, before.get(key));
            }
        }
    }

    public void addAndCheckCommit(TapRecordEvent recordEvent, WriteListResult<TapRecordEvent> listResult) {
        batchCache.add(recordEvent);
        if (batchCache.size() >= 1000) {
            executeBatch(listResult);
        }
    }

    public void releaseResource() {
        try {
            if (EmptyKit.isNotNull(preparedStatement)) {
                preparedStatement.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public PreparedStatement getPreparedStatement() {
        return preparedStatement;
    }

    public void setPreparedStatement(PreparedStatement preparedStatement) {
        this.preparedStatement = preparedStatement;
    }

    public AtomicLong getAtomicLong() {
        return atomicLong;
    }

    public void setAtomicLong(AtomicLong atomicLong) {
        this.atomicLong = atomicLong;
    }

    public List<TapRecordEvent> getBatchCache() {
        return batchCache;
    }

    public void setBatchCache(List<TapRecordEvent> batchCache) {
        this.batchCache = batchCache;
    }

    public String getPostgresVersion() {
        return postgresVersion;
    }

    public void setPostgresVersion(String postgresVersion) {
        this.postgresVersion = postgresVersion;
    }
}
