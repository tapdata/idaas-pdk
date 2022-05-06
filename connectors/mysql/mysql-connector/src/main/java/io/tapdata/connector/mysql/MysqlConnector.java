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
import io.tapdata.pdk.apis.TapConnector;
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
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * @author samuel
 * @Description
 * @create 2022-04-25 15:09
 **/
@TapConnectorClass("spec.json")
public class MysqlConnector extends ConnectorBase implements TapConnector {

	private static final String TAG = MysqlConnector.class.getSimpleName();
	private MysqlJdbcContext mysqlJdbcContext;
	private MysqlReader mysqlReader;
	private AtomicBoolean running;
	private String version;

	@Override
	public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecRegistry codecRegistry) {
		codecRegistry.registerFromTapValue(TapMapValue.class, "json", tapValue -> toJson(tapValue.getOriginValue()));
		codecRegistry.registerFromTapValue(TapArrayValue.class, "json", tapValue -> toJson(tapValue.getOriginValue()));
		connectorFunctions.supportBatchRead(this::batchRead);
		connectorFunctions.supportBatchCount(this::batchCount);
		connectorFunctions.supportStreamRead(this::streamRead);
		connectorFunctions.supportStreamOffset(this::streamOffset);
		connectorFunctions.supportQueryByAdvanceFilter(this::query);
		connectorFunctions.supportWriteRecord(this::writeRecord);
		connectorFunctions.supportInit(this::init);
	}

	private void init(TapConnectorContext tapConnectorContext) throws Throwable {
		this.mysqlJdbcContext = new MysqlJdbcContext(tapConnectorContext);
		this.mysqlReader = new MysqlReader(mysqlJdbcContext);
		this.running.set(true);
		this.version = mysqlJdbcContext.getMysqlVersion();
	}

	private void writeRecord(TapConnectorContext tapConnectorContext, List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) {

	}

	private void query(TapConnectorContext tapConnectorContext, TapAdvanceFilter tapAdvanceFilter, Consumer<FilterResults> filterResultsConsumer) {

	}

	private String streamOffset(TapConnectorContext tapConnectorContext, List<String> tableList, Long offsetStartTime) {
		return null;
	}

	private void streamRead(TapConnectorContext tapConnectorContext, List<String> tables, String offset, int recordSize, StreamReadConsumer streamReadConsumer) {

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

	private void batchRead(TapConnectorContext tapConnectorContext, TapTable tapTable, String offset, int eventBatchSize, BiConsumer<List<TapEvent>, String> biConsumer) throws Throwable {
		MysqlSnapshotOffset mysqlSnapshotOffset;
		if (StringUtils.isNotBlank(offset)) {
			mysqlSnapshotOffset = fromJson(offset, MysqlSnapshotOffset.class);
		} else {
			mysqlSnapshotOffset = new MysqlSnapshotOffset();
		}
		List<TapEvent> tempList = new ArrayList<>();
		this.mysqlReader.batchRead(tapConnectorContext, tapTable, mysqlSnapshotOffset, null, (data, snapshotOffset) -> {
			TapRecordEvent tapRecordEvent = tapRecordWrapper(tapConnectorContext, data);
			tempList.add(tapRecordEvent);
			if (tempList.size() == eventBatchSize) {
				biConsumer.accept(tempList, "");
				tempList.clear();
			}
		});
		if (CollectionUtils.isNotEmpty(tempList)) {
			biConsumer.accept(tempList, "");
			tempList.clear();
		}
	}

	private TapRecordEvent tapRecordWrapper(TapConnectorContext tapConnectorContext, Map<String, Object> data) {
		TapRecordEvent tapEvent = new TapRecordEvent();
		tapEvent.setConnector(tapConnectorContext.getSpecification().getId());
		tapEvent.setConnectorVersion(version);
		tapEvent.setInfo(data);
		tapEvent.setTime(System.currentTimeMillis());
		return tapEvent;
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

	@Override
	public void destroy() {
		running.compareAndSet(true, false);
		try {
			this.mysqlJdbcContext.close();
		} catch (Exception e) {
			TapLogger.error(TAG, "Release connector failed, error: " + e.getMessage() + "\n" + getStackString(e));
		}
	}
}
