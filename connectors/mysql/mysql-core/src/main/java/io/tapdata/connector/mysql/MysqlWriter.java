package io.tapdata.connector.mysql;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.collections4.map.LRUMap;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author samuel
 * @Description
 * @create 2022-05-05 21:18
 **/
public abstract class MysqlWriter {

	private static final String TAG = MysqlWriter.class.getSimpleName();
	protected static final String INSERT_SQL_TEMPLATE = "INSERT INTO `%s`.`%s`(%s) values(%s)";
	protected static final String UPDATE_SQL_TEMPLATE = "UPDATE `%s`.`%s` SET %s WHERE %s";
	protected static final String DELETE_SQL_TEMPLATE = "DELETE FROM `%s`.`%s` WHERE %s";
	protected static final String CHECK_ROW_EXISTS_TEMPLATE = "SELECT COUNT(1) FROM `%s`.`%s` WHERE %s";
	protected MysqlJdbcContext mysqlJdbcContext;

	public MysqlWriter(MysqlJdbcContext mysqlJdbcContext) {
		this.mysqlJdbcContext = mysqlJdbcContext;
	}

	abstract public void write(TapConnectorContext tapConnectorContext, List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer);

	protected String getKey(TapTable tapTable) {
		LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
		Set<String> keys = nameFieldMap.keySet();
		String keyString = String.join("-", keys);
		return tapTable.getId() + "-" + keyString;
	}

	protected PreparedStatement getInsertPreparedStatement(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent, Map<String, PreparedStatement> insertMap) throws Throwable {
		String key = getKey(tapTable);
		PreparedStatement preparedStatement = insertMap.get(key);
		if (null == preparedStatement) {
			DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
			String database = connectionConfig.getString("database");
			String name = connectionConfig.getString("name");
			String tableId = tapTable.getId();
			LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
			if (MapUtils.isEmpty(nameFieldMap)) {
				throw new Exception("Create insert prepared statement error, table \"" + tableId + "\"'s fields is empty, retry after reload connection \"" + name + "\"'s schema");
			}
			List<String> fields = new ArrayList<>();
			nameFieldMap.keySet().forEach(f -> fields.add("`" + f + "`"));
			List<String> questionMarks = fields.stream().map(f -> "?").collect(Collectors.toList());
			String sql = String.format(INSERT_SQL_TEMPLATE, database, tableId, String.join(",", fields), String.join(",", questionMarks));
			try {
				preparedStatement = this.mysqlJdbcContext.getConnection().prepareStatement(sql);
			} catch (SQLException e) {
				throw new Exception("Create insert prepared statement error, sql: " + sql + ", message: " + e.getSQLState() + " " + e.getErrorCode() + " " + e.getMessage(), e);
			}
			insertMap.put(key, preparedStatement);
		}
		return preparedStatement;
	}

	protected static class LRUOnRemoveMap<K, V> extends LRUMap<K, V> {

		private Consumer<Entry<K, V>> onRemove;

		public LRUOnRemoveMap(int maxSize, Consumer<Entry<K, V>> onRemove) {
			super(maxSize);
			this.onRemove = onRemove;
		}

		@Override
		protected boolean removeLRU(LinkEntry<K, V> entry) {
			onRemove.accept(entry);
			return super.removeLRU(entry);
		}

		@Override
		public void clear() {
			Set<Entry<K, V>> entries = this.entrySet();
			for (Entry<K, V> entry : entries) {
				onRemove.accept(entry);
			}
			super.clear();
		}

		@Override
		protected void removeEntry(HashEntry<K, V> entry, int hashIndex, HashEntry<K, V> previous) {
			onRemove.accept(entry);
			super.removeEntry(entry, hashIndex, previous);
		}

		@Override
		protected void removeMapping(HashEntry<K, V> entry, int hashIndex, HashEntry<K, V> previous) {
			onRemove.accept(entry);
			super.removeMapping(entry, hashIndex, previous);
		}
	}
}
