package io.tapdata.common;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class WriteRecorder {

    protected final Connection connection;
    protected final TapTable tapTable;
    protected final List<String> allColumn;
    protected final String schema;
    protected List<String> uniqueCondition;
    protected boolean hasPk = false;
    protected boolean uniqueConditionIsIndex = false;
    protected String version;

    protected PreparedStatement preparedStatement = null;
    protected final AtomicLong atomicLong = new AtomicLong(0);
    protected final List<TapRecordEvent> batchCache = TapSimplify.list();

    public WriteRecorder(Connection connection, TapTable tapTable, String schema) {
        this.connection = connection;
        this.tapTable = tapTable;
        this.schema = schema;
        this.allColumn = new ArrayList<>(tapTable.getNameFieldMap().keySet());
        analyzeTable();
    }

    private void analyzeTable() {
        //1、primaryKeys has first priority
        if (EmptyKit.isNotEmpty(tapTable.primaryKeys(false))) {
            hasPk = true;
            uniqueCondition = new ArrayList<>(tapTable.primaryKeys(false));
        }
        //2、second priority: analyze table with its indexes
        else {
            uniqueConditionIsIndex = EmptyKit.isNotEmpty(tapTable.getIndexList()) && tapTable.getIndexList().stream().filter(TapIndex::isUnique).anyMatch(in ->
                    (in.getIndexFields().size() == uniqueCondition.size()) && new HashSet<>(uniqueCondition)
                            .containsAll(in.getIndexFields().stream().map(TapIndexField::getName).collect(Collectors.toList())));
            uniqueCondition = new ArrayList<>(tapTable.primaryKeys(true));
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

    public void setVersion(String version) {
        this.version = version;
    }

    public AtomicLong getAtomicLong() {
        return atomicLong;
    }

    public abstract void addInsertBatch(Map<String, Object> after) throws SQLException;

    public abstract void addUpdateBatch(Map<String, Object> after) throws SQLException;

    public abstract void addDeleteBatch(Map<String, Object> before) throws SQLException;
}
