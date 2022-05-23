package io.tapdata.connector.mysql;

import io.tapdata.connector.mysql.util.JdbcUtil;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author samuel
 * @Description
 * @create 2022-05-06 10:36
 **/
public class MysqlJdbcOneByOneWriter extends MysqlWriter {

	private static final String TAG = MysqlJdbcOneByOneWriter.class.getSimpleName();
	private final Map<String, PreparedStatement> insertMap = new LRUOnRemoveMap<>(10, entry -> JdbcUtil.closeQuietly(entry.getValue()));
	private final Map<String, PreparedStatement> updateMap = new LRUOnRemoveMap<>(10, entry -> JdbcUtil.closeQuietly(entry.getValue()));
	private final Map<String, PreparedStatement> deleteMap = new LRUOnRemoveMap<>(10, entry -> JdbcUtil.closeQuietly(entry.getValue()));
	private final Map<String, PreparedStatement> checkExistsMap = new LRUOnRemoveMap<>(10, entry -> JdbcUtil.closeQuietly(entry.getValue()));
	private AtomicBoolean running = new AtomicBoolean(true);

	public MysqlJdbcOneByOneWriter(MysqlJdbcContext mysqlJdbcContext) throws Throwable {
		super(mysqlJdbcContext);
	}

	@Override
	public WriteListResult<TapRecordEvent> write(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents) throws Throwable {
		WriteListResult<TapRecordEvent> writeListResult = new WriteListResult<>(0L, 0L, 0L, new HashMap<>());
		try {
			for (TapRecordEvent tapRecordEvent : tapRecordEvents) {
				if (!running.get()) {
					break;
				}
				if (tapRecordEvent instanceof TapInsertRecordEvent) {
					int insertRow = doInsertOne(tapConnectorContext, tapTable, tapRecordEvent, writeListResult);
					writeListResult.incrementInserted(insertRow);
				} else if (tapRecordEvent instanceof TapUpdateRecordEvent) {
					int updateRow = doUpdateOne(tapConnectorContext, tapTable, tapRecordEvent, writeListResult);
					writeListResult.incrementModified(updateRow);
				} else if (tapRecordEvent instanceof TapDeleteRecordEvent) {
					int deleteRow = doDeleteOne(tapConnectorContext, tapTable, tapRecordEvent, writeListResult);
					writeListResult.incrementRemove(deleteRow);
				} else {
					writeListResult.addError(tapRecordEvent, new Exception("Event type \"" + tapRecordEvent.getClass().getSimpleName() + "\" not support: " + tapRecordEvent));
				}
			}
			MysqlJdbcContext.tryCommit(connection);
		} catch (Throwable e) {
			writeListResult.setInsertedCount(0);
			writeListResult.setModifiedCount(0);
			writeListResult.setRemovedCount(0);
			MysqlJdbcContext.tryRollBack(connection);
			throw e;
		}
		return writeListResult;
	}

	@Override
	public void onDestroy() {
		this.running.set(false);
		this.insertMap.clear();
		this.updateMap.clear();
		this.deleteMap.clear();
		this.checkExistsMap.clear();
	}

	private int doInsertOne(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent, WriteListResult<TapRecordEvent> writeListResult) throws Throwable {
		PreparedStatement insertPreparedStatement = getInsertPreparedStatement(tapConnectorContext, tapTable, tapRecordEvent, insertMap);
		setPreparedStatementValues(tapTable, tapRecordEvent, insertPreparedStatement);
		int row = 0;
		try {
			row = insertPreparedStatement.executeUpdate();
			writeListResult.incrementInserted(row);
		} catch (Exception e) {
			TapLogger.warn(TAG, "Execute insert failed, will retry update or insert after check record exists");
			if (rowExists(tapConnectorContext, tapTable, tapRecordEvent)) {
				row = doUpdateOne(tapConnectorContext, tapTable, tapRecordEvent, writeListResult);
			} else {
				writeListResult.addError(tapRecordEvent, new Exception("Insert data failed, message: " + e.getMessage(), e));
			}
		}
		return row;
	}

	private int doUpdateOne(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent, WriteListResult<TapRecordEvent> writeListResult) throws Throwable {
		PreparedStatement updatePreparedStatement = getUpdatePreparedStatement(tapConnectorContext, tapTable, tapRecordEvent, updateMap);
		int parameterIndex = setPreparedStatementValues(tapTable, tapRecordEvent, updatePreparedStatement);
		setPreparedStatementWhere(tapTable, tapRecordEvent, updatePreparedStatement, parameterIndex);
		int row = 0;
		try {
			row = updatePreparedStatement.executeUpdate();
		} catch (Exception e) {
			writeListResult.addError(tapRecordEvent, new Exception("Update data failed, message: " + e.getMessage(), e));
		}
		return row;
	}

	private int doDeleteOne(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent, WriteListResult<TapRecordEvent> writeListResult) throws Throwable {
		PreparedStatement deletePreparedStatement = getDeletePreparedStatement(tapConnectorContext, tapTable, tapRecordEvent, deleteMap);
		setPreparedStatementWhere(tapTable, tapRecordEvent, deletePreparedStatement, 1);
		int row = 0;
		try {
			row = deletePreparedStatement.executeUpdate();
			writeListResult.incrementRemove(row);
		} catch (SQLException e) {
			writeListResult.addError(tapRecordEvent, new Exception("Delete data failed, message: " + e.getMessage(), e));
		}
		return row;
	}

	private boolean rowExists(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
		PreparedStatement checkRowExistsPreparedStatement = getCheckRowExistsPreparedStatement(tapConnectorContext, tapTable, tapRecordEvent, checkExistsMap);
		setPreparedStatementWhere(tapTable, tapRecordEvent, checkRowExistsPreparedStatement, 1);
		AtomicBoolean result = new AtomicBoolean(false);
		this.mysqlJdbcContext.query(checkRowExistsPreparedStatement, rs -> {
			if (rs.next()) {
				int count = rs.getInt("count");
				result.set(count > 0);
			}
		});
		return result.get();
	}
}
