package io.tapdata.connector.mysql;

import io.tapdata.connector.mysql.entity.MysqlSnapshotOffset;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import java.sql.ResultSetMetaData;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

/**
 * @author samuel
 * @Description
 * @create 2022-05-05 20:13
 **/
public class MysqlReader {

	private static final String TAG = MysqlReader.class.getSimpleName();
	private MysqlJdbcContext mysqlJdbcContext;

	public MysqlReader(MysqlJdbcContext mysqlJdbcContext) {
		this.mysqlJdbcContext = mysqlJdbcContext;
	}

	public void batchRead(TapConnectorContext tapConnectorContext, TapTable tapTable, MysqlSnapshotOffset mysqlSnapshotOffset, Predicate<?> stop,
						  BiConsumer<Map<String, Object>, MysqlSnapshotOffset> biConsumer) throws Throwable {
		DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
		String database = connectionConfig.getString("database");
		String sql = String.format(MysqlJdbcContext.SELECT_TABLE, database, tapTable.getName());
		Collection<String> pks = tapTable.primaryKeys();
		List<String> whereList = new ArrayList<>();
		List<String> orderList = new ArrayList<>();
		if (MapUtils.isNotEmpty(mysqlSnapshotOffset.getOffset())) {
			for (Map.Entry<String, Object> entry : mysqlSnapshotOffset.getOffset().entrySet()) {
				String key = entry.getKey();
				Object value = entry.getValue();
				if (value instanceof Number) {
					whereList.add(key + ">" + value);
				} else {
					whereList.add(key + ">'" + value + "'");
				}
				orderList.add(key + " ASC");
			}
		}
		if (CollectionUtils.isNotEmpty(pks)) {
			for (String pk : pks) {
				orderList.add(pk + " ASC");
			}
		} else {
			TapLogger.info(TAG, "Table {} not support snapshot offset", tapTable.getName());
		}
		if (CollectionUtils.isNotEmpty(whereList)) {
			sql += " WHERE " + String.join(" AND ", whereList);
		}
		if (CollectionUtils.isNotEmpty(orderList)) {
			sql += " ORDER BY " + String.join(",", orderList);
		}
		AtomicLong row = new AtomicLong(0L);
		try {
			this.mysqlJdbcContext.queryWithStream(sql, rs -> {
				ResultSetMetaData metaData = rs.getMetaData();
				while (rs.next()) {
					if (null != stop && stop.test(null)) {
						break;
					}
					row.incrementAndGet();
					Map<String, Object> data = new HashMap<>();
					for (int i = 0; i < metaData.getColumnCount(); i++) {
						String columnName = metaData.getColumnName(i + 1);
						try {
							Object value = rs.getObject(i + 1);
							data.put(columnName, value);
							if (pks.contains(columnName)) {
								mysqlSnapshotOffset.getOffset().put(columnName, value);
							}
						} catch (Exception e) {
							throw new Exception("Read column value failed, row: " + row.get() + ", column name: " + columnName + ", data: " + data + "; Error: " + e.getMessage(), e);
						}
					}
					biConsumer.accept(data, mysqlSnapshotOffset);
				}
			});
		} catch (Exception e) {
			throw new Exception("Batch read table " + tapTable.getName() + " failed, sql: " + sql + "; Error: " + e.getMessage(), e);
		}
	}
}
