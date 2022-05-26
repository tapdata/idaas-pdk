package io.tapdata.connector.postgres.storage;

import io.tapdata.connector.postgres.PostgresSqlMaker;
import io.tapdata.connector.postgres.kit.EmptyKit;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PostgresWriteRecorder {

    private final Connection connection;
    private PreparedStatement preparedStatement = null;
    private AtomicLong atomicLong = new AtomicLong(0);
    private List<TapRecordEvent> batchCache = TapSimplify.list();

    public PostgresWriteRecorder(Connection connection) {
        this.connection = connection;
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

    public void addInsertBatch(TapTable tapTable, Map<String, Object> after) throws SQLException {
        if (EmptyKit.isEmpty(after)) {
            return;
        }
        if (EmptyKit.isNull(preparedStatement)) {
            preparedStatement = connection.prepareStatement(PostgresSqlMaker.buildPrepareInsertSQL(tapTable));
        }
        preparedStatement.clearParameters();
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        List<String> columnList = nameFieldMap.entrySet().stream().sorted(Comparator.comparing(v -> v.getValue().getPos())).map(Map.Entry::getKey).collect(Collectors.toList());
        int pos = 1;
        for (String columnName : columnList) {
            TapField tapField = nameFieldMap.get(columnName);
            Object tapValue = after.get(columnName);
            if (tapField.getDataType() == null) {
                continue;
            }
            if (tapValue == null) {
                if (tapField.getNullable() != null && !tapField.getNullable()) {
                    preparedStatement.setObject(pos++, tapField.getDefaultValue());
                } else {
                    preparedStatement.setObject(pos++, null);
                }
            } else {
                preparedStatement.setObject(pos++, tapValue);
            }
        }
        preparedStatement.addBatch();
    }

    public void addUpdateBatch(TapTable tapTable, Map<String, Object> before, Map<String, Object> after, Collection<String> keys) throws SQLException {
        if (EmptyKit.isEmpty(before) || EmptyKit.isEmpty(after)) {
            return;
        }
        for (Map.Entry<String, Object> entry : before.entrySet()) {
            after.remove(entry.getKey(), entry.getValue());
        }
        if(EmptyKit.isNotEmpty(keys)) {
            before.keySet().removeIf(k -> !tapTable.primaryKeys().contains(k));
        }
        if (EmptyKit.isNull(preparedStatement)) {
            preparedStatement = connection.prepareStatement("UPDATE \"" + tapTable.getId() + "\" SET " +
                    after.keySet().stream().map(k -> "\"" + k + "\"=?").reduce((v1, v2) -> v1 + ", " + v2).orElseGet(String::new) + " WHERE " +
                    before.keySet().stream().map(k -> "\"" + k + "\"=?").reduce((v1, v2) -> v1 + " AND " + v2).orElseGet(String::new));
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : after.keySet()) {
            preparedStatement.setObject(pos++, after.get(key));
        }
        for (String key : before.keySet()) {
            preparedStatement.setObject(pos++, before.get(key));
        }
        preparedStatement.addBatch();
    }

    public void addDeleteBatch(TapTable tapTable, Map<String, Object> before, Collection<String> keys) throws SQLException {
        if (EmptyKit.isEmpty(before)) {
            return;
        }
        if(EmptyKit.isNotEmpty(keys)) {
            before.keySet().removeIf(k -> !tapTable.primaryKeys().contains(k));
        }
        if (EmptyKit.isNull(preparedStatement)) {
            preparedStatement = connection.prepareStatement("DELETE FROM \"" + tapTable.getId() + "\" WHERE " +
                    before.keySet().stream().map(k -> "\"" + k + "\"=?").reduce((v1, v2) -> v1 + " AND " + v2).orElseGet(String::new));
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : before.keySet()) {
            preparedStatement.setObject(pos++, before.get(key));
        }
        preparedStatement.addBatch();
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
}
