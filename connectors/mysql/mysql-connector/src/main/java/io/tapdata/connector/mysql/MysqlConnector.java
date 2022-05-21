package io.tapdata.connector.mysql;

import io.tapdata.base.ConnectorBase;
import io.tapdata.connector.mysql.entity.MysqlSnapshotOffset;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.FilterResults;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * @author samuel
 * @Description
 * @create 2022-04-25 15:09
 **/
@TapConnectorClass("spec.json")
public class MysqlConnector extends ConnectorBase {

	private static final String TAG = MysqlConnector.class.getSimpleName();
	private static final int MAX_FILTER_RESULT_SIZE = 100;
	private MysqlJdbcContext mysqlJdbcContext;
	private MysqlReader mysqlReader;
	private MysqlWriter mysqlWriter;
	private String version;

	@Override
	public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
		codecRegistry.registerFromTapValue(TapMapValue.class, "json", tapValue -> toJson(tapValue.getValue()));
		codecRegistry.registerFromTapValue(TapArrayValue.class, "json", tapValue -> toJson(tapValue.getValue()));
		codecRegistry.registerFromTapValue(TapBooleanValue.class, "tinyint(1)", TapValue::getValue);
		codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> formatTapDateTime(tapTimeValue.getValue(), "HH:mm:ss.SSSSSS"));
		codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> formatTapDateTime(tapDateTimeValue.getValue(), "yyyy-MM-dd HH:mm:ss.SSSSSS"));
		codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> formatTapDateTime(tapDateValue.getValue(), "yyyy-MM-dd"));

		connectorFunctions.supportCreateTable(this::createTable);
		connectorFunctions.supportDropTable(this::dropTable);
		connectorFunctions.supportClearTable(this::clearTable);
		connectorFunctions.supportBatchCount(this::batchCount);
		connectorFunctions.supportBatchRead(this::batchRead);
//		connectorFunctions.supportStreamRead(this::streamRead);
//		connectorFunctions.supportStreamOffset(this::streamOffset);
		connectorFunctions.supportQueryByAdvanceFilter(this::query);
		connectorFunctions.supportWriteRecord(this::writeRecord);
		connectorFunctions.supportCreateIndex(this::createIndex);
	}

	private void createIndex(TapConnectorContext tapConnectorContext, TapTable tapTable, TapCreateIndexEvent tapCreateIndexEvent) throws Throwable {
		List<TapIndex> indexList = tapCreateIndexEvent.getIndexList();
		SqlMaker sqlMaker = new MysqlMaker();
		for (TapIndex tapIndex : indexList) {
			String createIndexSql;
			try {
				createIndexSql = sqlMaker.createIndex(tapConnectorContext, tapTable, tapIndex);
			} catch (Throwable e) {
				throw new RuntimeException("Get create index sql failed, message: " + e.getMessage(), e);
			}
			try {
				this.mysqlJdbcContext.execute(createIndexSql);
			} catch (Throwable e) {
				throw new RuntimeException("Execute create index failed, sql: " + createIndexSql + ", message: " + e.getMessage(), e);
			}
		}
	}

	@Override
	public int tableCount(TapConnectionContext connectionContext) throws Throwable {
		DataMap connectionConfig = connectionContext.getConnectionConfig();
		String database = connectionConfig.getString("database");
		AtomicInteger count = new AtomicInteger(0);
		this.mysqlJdbcContext.query(String.format("SELECT COUNT(1) count FROM `information_schema`.`TABLES` WHERE TABLE_SCHEMA='%s' AND TABLE_TYPE='BASE TABLE'", database), rs -> {
			if (rs.next()) {
				count.set(Integer.parseInt(rs.getString("count")));
			}
		});
		return count.get();
	}

	@Override
	public void onStart(TapConnectionContext tapConnectionContext) throws Throwable {
		this.mysqlJdbcContext = new MysqlJdbcContext(tapConnectionContext);
		if (tapConnectionContext instanceof TapConnectorContext) {
			this.mysqlWriter = new MysqlJdbcOneByOneWriter(mysqlJdbcContext);
			this.mysqlReader = new MysqlReader(mysqlJdbcContext);
			this.version = mysqlJdbcContext.getMysqlVersion();
		}
	}

	@Override
	public void onDestroy() throws Throwable {
		try {
			this.mysqlJdbcContext.close();
		} catch (Exception e) {
			TapLogger.error(TAG, "Release connector failed, error: " + e.getMessage() + "\n" + getStackString(e));
		}
		Optional.ofNullable(this.mysqlWriter).ifPresent(MysqlWriter::onDestroy);
	}

    @Override
    public void onPause() throws Throwable {

    }

    private void clearTable(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) throws Throwable {
		String tableId = tapClearTableEvent.getTableId();
		if (mysqlJdbcContext.tableExists(tableId)) {
			mysqlJdbcContext.clearTable(tableId);
		} else {
			DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
			String database = connectionConfig.getString("database");
			TapLogger.warn(TAG, "Table \"{}.{}\" not exists, will skip clear table", database, tableId);
		}
	}

	private void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) throws Throwable {
		mysqlJdbcContext.dropTable(tapDropTableEvent.getTableId());
	}

	private void createTable(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) throws Throwable {
		try {
			if (mysqlJdbcContext.tableExists(tapCreateTableEvent.getTableId())) {
				DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
				String database = connectionConfig.getString("database");
				String tableId = tapCreateTableEvent.getTableId();
				TapLogger.info(TAG, "Table \"{}.{}\" exists, skip auto create table", database, tableId);
			} else {
				String mysqlVersion = mysqlJdbcContext.getMysqlVersion();
				SqlMaker sqlMaker = new MysqlMaker();
				if (null == tapCreateTableEvent.getTable()) {
					TapLogger.warn(TAG, "Create table event's tap table is null, will skip it: " + tapCreateTableEvent);
					return;
				}
				String[] createTableSqls = sqlMaker.createTable(tapConnectorContext, tapCreateTableEvent, mysqlVersion);
				for (String createTableSql : createTableSqls) {
					try {
						mysqlJdbcContext.execute(createTableSql);
					} catch (Throwable e) {
						throw new Exception("Execute create table failed, sql: " + createTableSql + ", message: " + e.getMessage(), e);
					}
				}
			}
		} catch (Throwable t) {
			throw new Exception("Create table failed, message: " + t.getMessage(), t);
		}
	}

	private void writeRecord(TapConnectorContext tapConnectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> consumer) throws Throwable {
		WriteListResult<TapRecordEvent> writeListResult = this.mysqlWriter.write(tapConnectorContext, tapTable, tapRecordEvents);
		consumer.accept(writeListResult);
	}

	private void batchRead(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offset, int batchSize, BiConsumer<List<TapEvent>, Object> consumer) throws Throwable {
		MysqlSnapshotOffset mysqlSnapshotOffset;
		if (offset instanceof MysqlSnapshotOffset) {
			mysqlSnapshotOffset = (MysqlSnapshotOffset) offset;
		} else {
			mysqlSnapshotOffset = new MysqlSnapshotOffset();
		}
		List<TapEvent> tempList = new ArrayList<>();
		this.mysqlReader.readWithOffset(tapConnectorContext, tapTable, mysqlSnapshotOffset, n -> !isAlive(), (data, snapshotOffset) -> {
			TapRecordEvent tapRecordEvent = tapRecordWrapper(tapConnectorContext, null, data, tapTable, "i");
			tempList.add(tapRecordEvent);
			if (tempList.size() == batchSize) {
				consumer.accept(tempList, mysqlSnapshotOffset);
				tempList.clear();
			}
		});
		if (CollectionUtils.isNotEmpty(tempList)) {
			consumer.accept(tempList, mysqlSnapshotOffset);
			tempList.clear();
		}
	}

	private void query(TapConnectorContext tapConnectorContext, TapAdvanceFilter tapAdvanceFilter, TapTable tapTable, Consumer<FilterResults> consumer) throws Throwable {
		FilterResults filterResults = new FilterResults();
		filterResults.setFilter(tapAdvanceFilter);
		try {
			this.mysqlReader.readWithFilter(tapConnectorContext, tapTable, tapAdvanceFilter, n -> !isAlive(), data -> {
				filterResults.add(data);
				if (filterResults.getResults().size() == MAX_FILTER_RESULT_SIZE) {
					consumer.accept(filterResults);
					filterResults.getResults().clear();
				}
			});
			if (CollectionUtils.isNotEmpty(filterResults.getResults())) {
				consumer.accept(filterResults);
				filterResults.getResults().clear();
			}
		} catch (Throwable e) {
			filterResults.setError(e);
			consumer.accept(filterResults);
		}
	}

	private void streamRead(TapConnectorContext tapConnectorContext, List<String> tables, Object offset, int batchSize, StreamReadConsumer consumer) {

	}

	private void streamOffset(TapConnectorContext tapConnectorContext, List<String> tableList, Long offsetStartTime, BiConsumer<Object, Long> offsetOffsetTimeConsumer) {

	}

	private long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Throwable {
		int count;
		try {
			count = mysqlJdbcContext.count(tapTable.getName());
		} catch (Exception e) {
			throw new RuntimeException("Count table " + tapTable.getName() + " error: " + e.getMessage(), e);
		}
		return count;
	}

	private TapRecordEvent tapRecordWrapper(TapConnectorContext tapConnectorContext, Map<String, Object> before, Map<String, Object> after, TapTable tapTable, String op) {
		TapRecordEvent tapRecordEvent;
		switch (op) {
			case "i":
				tapRecordEvent = TapSimplify.insertRecordEvent(after, tapTable.getId());
				break;
			case "u":
				tapRecordEvent = TapSimplify.updateDMLEvent(before, after, tapTable.getId());
				break;
			case "d":
				tapRecordEvent = TapSimplify.deleteDMLEvent(before, tapTable.getId());
				break;
			default:
				throw new IllegalArgumentException("Operation " + op + " not support");
		}
		tapRecordEvent.setConnector(tapConnectorContext.getSpecification().getId());
		tapRecordEvent.setConnectorVersion(version);
		return tapRecordEvent;
	}

	@Override
	public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
		MysqlSchemaLoader mysqlSchemaLoader = new MysqlSchemaLoader(mysqlJdbcContext);
		mysqlSchemaLoader.discoverSchema(tables, consumer, tableSize);
	}

	@Override
	public void connectionTest(TapConnectionContext databaseContext, Consumer<TestItem> consumer) throws Throwable {
		MysqlConnectionTest mysqlConnectionTest = new MysqlConnectionTest(mysqlJdbcContext);
		TestItem testHostPort = mysqlConnectionTest.testHostPort(databaseContext);
		consumer.accept(testHostPort);
		if (testHostPort.getResult() == TestItem.RESULT_FAILED) {
			return;
		}
		TestItem testConnect = mysqlConnectionTest.testConnect();
		consumer.accept(testConnect);
		if (testConnect.getResult() == TestItem.RESULT_FAILED) {
			return;
		}
		TestItem testDatabaseVersion = mysqlConnectionTest.testDatabaseVersion();
		consumer.accept(testDatabaseVersion);
		if (testDatabaseVersion.getResult() == TestItem.RESULT_FAILED) {
			return;
		}
		consumer.accept(mysqlConnectionTest.testBinlogMode());
		consumer.accept(mysqlConnectionTest.testBinlogRowImage());
		consumer.accept(mysqlConnectionTest.testCDCPrivileges());
		consumer.accept(mysqlConnectionTest.testCreateTablePrivilege(databaseContext));
	}

}
