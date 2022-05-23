package io.tapdata.connector.mysql;

import io.debezium.config.Configuration;
import io.tapdata.connector.mysql.entity.MysqlSnapshotOffset;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;

import java.sql.ResultSetMetaData;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
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

	public void readWithOffset(TapConnectorContext tapConnectorContext, TapTable tapTable, MysqlSnapshotOffset mysqlSnapshotOffset,
							   Predicate<?> stop, BiConsumer<Map<String, Object>, MysqlSnapshotOffset> consumer) throws Throwable {
		SqlMaker sqlMaker = new MysqlMaker();
		String sql = sqlMaker.selectSql(tapConnectorContext, tapTable, mysqlSnapshotOffset);
		Collection<String> pks = tapTable.primaryKeys();
		AtomicLong row = new AtomicLong(0L);
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
				consumer.accept(data, mysqlSnapshotOffset);
			}
		});
	}

	public void readWithFilter(TapConnectorContext tapConnectorContext, TapTable tapTable, TapAdvanceFilter tapAdvanceFilter,
							   Predicate<?> stop, Consumer<Map<String, Object>> consumer) throws Throwable {
		SqlMaker sqlMaker = new MysqlMaker();
		String sql = sqlMaker.selectSql(tapConnectorContext, tapTable, tapAdvanceFilter);
		AtomicLong row = new AtomicLong(0L);
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
					} catch (Exception e) {
						throw new Exception("Read column value failed, row: " + row.get() + ", column name: " + columnName + ", data: " + data + "; Error: " + e.getMessage(), e);
					}
				}
				consumer.accept(data);
			}
		});
	}

	public void readBinlog(TapConnectorContext tapConnectorContext, List<String> tables, Object offset, int batchSize, StreamReadConsumer consumer) {
		KVMap<Object> stateMap = tapConnectorContext.getStateMap();
		DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
		Configuration.Builder builder = Configuration.create()
				.with("name", tapConnectorContext.getSpecification().getId())
				.with("connector.class", "io.debezium.connector.mysql.MySqlConnector")
				.with("database.hostname", connectionConfig.getString("host"))
				.with("database.port", Integer.parseInt(connectionConfig.getString("port")))
				.with("database.user", connectionConfig.getString("username"))
				.with("database.password", connectionConfig.getString("password"))
				.with("database.server.name", tapConnectorContext.getSpecification().getId())
				.with("database.whitelist", connectionConfig.getString("database"))
				.with("threadName", "Debezium-Mysql-Connector-" + tapConnectorContext.getSpecification().getId())
				.with("database.history.skip.unparseable.ddl", true)
				.with("database.history.store.only.monitored.tables.ddl", true)
				.with("snapshot.locking.mode", "none");
	}
}
