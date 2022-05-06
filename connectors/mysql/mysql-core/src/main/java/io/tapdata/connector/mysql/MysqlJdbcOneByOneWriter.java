package io.tapdata.connector.mysql;

import io.tapdata.connector.mysql.util.JdbcUtil;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * @author samuel
 * @Description
 * @create 2022-05-06 10:36
 **/
public class MysqlJdbcOneByOneWriter extends MysqlWriter {

	private static final String TAG = MysqlJdbcOneByOneWriter.class.getSimpleName();
	private Map<String, PreparedStatement> insertMap = new LRUOnRemoveMap<>(10, entry -> JdbcUtil.closeQuietly(entry.getValue()));
	private Map<String, PreparedStatement> updateMap = new LRUOnRemoveMap<>(10, entry -> JdbcUtil.closeQuietly(entry.getValue()));
	private Map<String, PreparedStatement> deleteMap = new LRUOnRemoveMap<>(10, entry -> JdbcUtil.closeQuietly(entry.getValue()));
	private Map<String, PreparedStatement> checkExistsMap = new LRUOnRemoveMap<>(10, entry -> JdbcUtil.closeQuietly(entry.getValue()));

	public MysqlJdbcOneByOneWriter(MysqlJdbcContext mysqlJdbcContext) {
		super(mysqlJdbcContext);
	}

	@Override
	public void write(TapConnectorContext tapConnectorContext, List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) {
		for (TapRecordEvent tapRecordEvent : tapRecordEvents) {
			if (tapRecordEvent instanceof TapInsertRecordEvent) {
			}
		}
	}
}
