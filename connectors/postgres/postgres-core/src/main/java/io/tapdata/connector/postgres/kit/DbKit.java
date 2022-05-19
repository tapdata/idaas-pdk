package io.tapdata.connector.postgres.kit;

import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class DbKit {

    public static List<DataMap> getDataFromResultSet(ResultSet resultSet) {
        List<DataMap> list = TapSimplify.list();
        try {
            if (EmptyKit.isNotNull(resultSet)) {
                List<String> columnNames = getColumnsFromResultSet(resultSet);
                while (!resultSet.isAfterLast()) {
                    list.add(getRowFromResultSet(resultSet, columnNames));
                    resultSet.next();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    public static DataMap getRowFromResultSet(ResultSet resultSet, Collection<String> columnNames) {
        DataMap map = DataMap.create();
        try {
            if (EmptyKit.isNotNull(resultSet) && resultSet.getRow() > 0) {
                for (String col : columnNames) {
                    map.put(col, resultSet.getObject(col));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return map;
    }

    public static List<String> getColumnsFromResultSet(ResultSet resultSet) {
        //get all column names
        List<String> columnNames = new ArrayList<>();
        try {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                columnNames.add(resultSetMetaData.getColumnName(i));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return columnNames;
    }
}
