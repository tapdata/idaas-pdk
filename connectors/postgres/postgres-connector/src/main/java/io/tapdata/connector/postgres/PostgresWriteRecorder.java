package io.tapdata.connector.postgres;

import io.tapdata.common.WriteRecorder;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class PostgresWriteRecorder extends WriteRecorder {

    public PostgresWriteRecorder(Connection connection, TapTable tapTable, String schema) {
        super(connection, tapTable, schema);
    }

    @Override
    public void addInsertBatch(Map<String, Object> after) throws SQLException {
        //after is empty will be skipped
        if (EmptyKit.isEmpty(after)) {
            return;
        }
        //insert into all columns, make preparedStatement
        if (EmptyKit.isNull(preparedStatement)) {
            String insertHead = "INSERT INTO \"" + schema + "\".\"" + tapTable.getId() + "\" ("
                    + allColumn.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", ")) + ") ";
            String insertValue = "VALUES(" + StringKit.copyString("?", allColumn.size(), ",") + ") ";
            String insertSql = insertHead + insertValue;
            if (EmptyKit.isNotEmpty(uniqueCondition)) {
                if (Integer.parseInt(version) > 90500 && uniqueConditionIsIndex) {
                    insertSql += "ON CONFLICT("
                            + uniqueCondition.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", "))
                            + ") DO UPDATE SET " + allColumn.stream().map(k -> "\"" + k + "\"=?").collect(Collectors.joining(", "));
                } else {
                    if (hasPk) {
                        insertSql = "WITH upsert AS (UPDATE \"" + schema + "\".\"" + tapTable.getId() + "\" SET " + allColumn.stream().map(k -> "\"" + k + "\"=?")
                                .collect(Collectors.joining(", ")) + " WHERE " + uniqueCondition.stream().map(k -> "\"" + k + "\"=?")
                                .collect(Collectors.joining(" AND ")) + " RETURNING *) " + insertHead + " SELECT "
                                + StringKit.copyString("?", allColumn.size(), ",") + " WHERE NOT EXISTS (SELECT * FROM upsert)";
                    } else {
                        insertSql = "WITH upsert AS (UPDATE \"" + schema + "\".\"" + tapTable.getId() + "\" SET " + allColumn.stream().map(k -> "\"" + k + "\"=?")
                                .collect(Collectors.joining(", ")) + " WHERE " + uniqueCondition.stream().map(k -> "(\"" + k + "\"=? OR (\"" + k + "\" IS NULL AND ?::text IS NULL))")
                                .collect(Collectors.joining(" AND ")) + " RETURNING *) " + insertHead + " SELECT "
                                + StringKit.copyString("?", allColumn.size(), ",") + " WHERE NOT EXISTS (SELECT * FROM upsert)";
                    }
                }
            }
            preparedStatement = connection.prepareStatement(insertSql);
        }
        preparedStatement.clearParameters();
        //make params
        int pos = 1;
        if ((Integer.parseInt(version) <= 90500 || !uniqueConditionIsIndex) && EmptyKit.isNotEmpty(uniqueCondition)) {
            for (String key : allColumn) {
                preparedStatement.setObject(pos++, after.get(key));
            }
            if(hasPk) {
                for (String key : uniqueCondition) {
                    preparedStatement.setObject(pos++, after.get(key));
                }
            }
            else {
                for (String key : uniqueCondition) {
                    preparedStatement.setObject(pos++, after.get(key));
                    preparedStatement.setObject(pos++, after.get(key));
                }
            }
        }
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, after.get(key));
        }
        if (EmptyKit.isNotEmpty(uniqueCondition) && Integer.parseInt(version) > 90500 && uniqueConditionIsIndex) {
            for (String key : allColumn) {
                preparedStatement.setObject(pos++, after.get(key));
            }
        }
        preparedStatement.addBatch();
    }

    //before is always empty
    @Override
    public void addUpdateBatch(Map<String, Object> after) throws SQLException {
        if (EmptyKit.isEmpty(after) || EmptyKit.isEmpty(uniqueCondition)) {
            return;
        }
        Map<String, Object> before = new HashMap<>();
        uniqueCondition.forEach(k -> before.put(k, after.get(k)));
        if (EmptyKit.isNull(preparedStatement)) {
            if (hasPk) {
                preparedStatement = connection.prepareStatement("UPDATE \"" + schema + "\".\"" + tapTable.getId() + "\" SET " +
                        allColumn.stream().map(k -> "\"" + k + "\"=?").collect(Collectors.joining(", ")) + " WHERE " +
                        before.keySet().stream().map(k -> "\"" + k + "\"=?").collect(Collectors.joining(" AND ")));
            } else {
                preparedStatement = connection.prepareStatement("UPDATE \"" + schema + "\".\"" + tapTable.getId() + "\" SET " +
                        allColumn.stream().map(k -> "\"" + k + "\"=?").collect(Collectors.joining(", ")) + " WHERE " +
                        before.keySet().stream().map(k -> "(\"" + k + "\"=? OR (\"" + k + "\" IS NULL AND ?::text IS NULL))")
                                .collect(Collectors.joining(" AND ")));
            }
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, after.get(key));
        }
        dealNullBefore(before, pos);
        preparedStatement.addBatch();
    }

    @Override
    public void addDeleteBatch(Map<String, Object> before) throws SQLException {
        if (EmptyKit.isEmpty(before)) {
            return;
        }
        if (EmptyKit.isNotEmpty(uniqueCondition)) {
            before.keySet().removeIf(k -> !uniqueCondition.contains(k));
        }
        if (EmptyKit.isNull(preparedStatement)) {
            if (hasPk) {
                preparedStatement = connection.prepareStatement("DELETE FROM \"" + schema + "\".\"" + tapTable.getId() + "\" WHERE " +
                        before.keySet().stream().map(k -> "\"" + k + "\"=?").collect(Collectors.joining(" AND ")));
            } else {
                preparedStatement = connection.prepareStatement("DELETE FROM \"" + schema + "\".\"" + tapTable.getId() + "\" WHERE " +
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
}
