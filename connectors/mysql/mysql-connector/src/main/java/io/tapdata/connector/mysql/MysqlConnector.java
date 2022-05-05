package io.tapdata.connector.mysql;

import io.tapdata.entity.codec.TapCodecRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapArrayValue;
import io.tapdata.entity.schema.value.TapMapValue;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.util.List;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.toJson;

/**
 * @author samuel
 * @Description
 * @create 2022-04-25 15:09
 **/
@TapConnectorClass("spec.json")
public class MysqlConnector implements TapConnector {

	@Override
	public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecRegistry codecRegistry) {
		codecRegistry.registerFromTapValue(TapMapValue.class, "json", tapValue -> toJson(tapValue.getOriginValue()));
		codecRegistry.registerFromTapValue(TapArrayValue.class, "json", tapValue -> toJson(tapValue.getOriginValue()));
		connectorFunctions.supportBatchRead(this::batchRead);
		connectorFunctions.supportBatchOffset(this::batchOffset);
		connectorFunctions.supportBatchCount(this::batchCount);
		connectorFunctions.supportStreamRead(this::streamRead);
		connectorFunctions.supportStreamOffset(this::streamOffset);
	}

	private long batchCount(TapConnectorContext tapConnectorContext, String s) {
		return 0;
	}

	private String streamOffset(TapConnectorContext tapConnectorContext, Long aLong) {
		return "";
	}

	private String batchOffset(TapConnectorContext tapConnectorContext) {
		return "";
	}

	private void streamRead(TapConnectorContext tapConnectorContext, String s, int i, StreamReadConsumer streamReadConsumer) {
	}

	private void batchRead(TapConnectorContext tapConnectorContext, String offsetState, int eventBatchSize, Consumer<List<TapEvent>> listConsumer) {

	}

	@Override
	public void discoverSchema(TapConnectionContext connectionContext, Consumer<List<TapTable>> consumer) throws Throwable {
		MysqlJdbcContext mysqlJdbcContext = new MysqlJdbcContext(connectionContext);
		MysqlSchemaLoader mysqlSchemaLoader = new MysqlSchemaLoader(mysqlJdbcContext);
		mysqlSchemaLoader.discoverSchema(consumer);
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

	}
}
