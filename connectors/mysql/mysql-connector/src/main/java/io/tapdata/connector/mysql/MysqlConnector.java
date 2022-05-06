package io.tapdata.connector.mysql;

import io.tapdata.base.ConnectorBase;
import io.tapdata.connector.mysql.entity.MysqlSnapshotOffset;
import io.tapdata.entity.codec.TapCodecRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapArrayValue;
import io.tapdata.entity.schema.value.TapMapValue;
import io.tapdata.entity.simplify.TapSimplify;
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
	private MysqlJdbcContext mysqlJdbcContext;
	private MysqlReader mysqlReader;
	private String version;

	@Override
	public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecRegistry codecRegistry) {
		codecRegistry.registerFromTapValue(TapMapValue.class, "json", tapValue -> toJson(tapValue.getOriginValue()));
		codecRegistry.registerFromTapValue(TapArrayValue.class, "json", tapValue -> toJson(tapValue.getOriginValue()));
		connectorFunctions.supportBatchCount(this::batchCount);
		connectorFunctions.supportBatchRead(this::batchRead);
		connectorFunctions.supportStreamRead(this::streamRead);
		connectorFunctions.supportStreamOffset(this::streamOffset);
		connectorFunctions.supportQueryByAdvanceFilter(this::query);
		connectorFunctions.supportWriteRecord(this::writeRecord);
	}

	private void streamRead(TapConnectorContext tapConnectorContext, List<String> tables, Object offset, int batchSize, StreamReadConsumer consumer) {

	}

	private void batchRead(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offset, int batchSize, BiConsumer<List<TapEvent>, Object> consumer) throws Throwable {
		MysqlSnapshotOffset mysqlSnapshotOffset;
		if (offset instanceof MysqlSnapshotOffset) {
			mysqlSnapshotOffset = (MysqlSnapshotOffset) offset;
		} else {
			mysqlSnapshotOffset = new MysqlSnapshotOffset();
		}
		List<TapEvent> tempList = new ArrayList<>();
		this.mysqlReader.batchRead(tapConnectorContext, tapTable, mysqlSnapshotOffset, n -> !isAlive(), (data, snapshotOffset) -> {
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

	@Override
	public void onStart(TapConnectorContext tapConnectorContext) throws Throwable {
		this.mysqlJdbcContext = new MysqlJdbcContext(tapConnectorContext);
		this.mysqlReader = new MysqlReader(mysqlJdbcContext);
		this.version = mysqlJdbcContext.getMysqlVersion();
	}

	@Override
	public void onDestroy() throws Throwable {
		try {
			this.mysqlJdbcContext.close();
		} catch (Exception e) {
			TapLogger.error(TAG, "Release connector failed, error: " + e.getMessage() + "\n" + getStackString(e));
		}
	}

	private void writeRecord(TapConnectorContext tapConnectorContext, List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) {

	}

	private void query(TapConnectorContext tapConnectorContext, TapAdvanceFilter tapAdvanceFilter, Consumer<FilterResults> filterResultsConsumer) {

	}

	private String streamOffset(TapConnectorContext tapConnectorContext, List<String> tableList, Long offsetStartTime) {
		return null;
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
		MysqlJdbcContext mysqlJdbcContext = new MysqlJdbcContext(connectionContext);
		MysqlSchemaLoader mysqlSchemaLoader = new MysqlSchemaLoader(mysqlJdbcContext);
		mysqlSchemaLoader.discoverSchema(consumer, tableSize);
	}

	@Override
	public void connectionTest(TapConnectionContext databaseContext, Consumer<TestItem> consumer) throws Throwable {
		MysqlJdbcContext mysqlJdbcContext = new MysqlJdbcContext(databaseContext);
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
