package io.tapdata.connector.mysql;

import io.tapdata.entity.conversion.TableFieldTypesGenerator;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * @author samuel
 * @Description
 * @create 2022-04-26 20:09
 **/
public class MysqlSchemaLoader {
	private static final String TAG = MysqlSchemaLoader.class.getSimpleName();
	private static final int BATCH_SIZE = 10;
	private static final String SELECT_TABLES = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s'";
	private static final String SELECT_COLUMNS = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME='%s'";
	private final static String SELECT_ALL_INDEX_SQL = "select i.TABLE_NAME, i.INDEX_NAME, i.INDEX_TYPE, i.COLLATION, i.NON_UNIQUE, i.COLUMN_NAME, i.SEQ_IN_INDEX from INFORMATION_SCHEMA.STATISTICS i\n" +
			"  left join INFORMATION_SCHEMA.KEY_COLUMN_USAGE k\n" +
			"    on i.TABLE_SCHEMA = k.TABLE_SCHEMA\n" +
			"   and i.TABLE_NAME = k.TABLE_NAME\n" +
			"   and i.INDEX_NAME = CONCAT(k.CONSTRAINT_NAME,'_idx')\n" +
			"   and i.COLUMN_NAME = k.COLUMN_NAME\n" +
			" where i.TABLE_SCHEMA = ?\n" +
			"   and i.TABLE_NAME = ?\n" +
			"   and i.INDEX_NAME <> 'PRIMARY'\n" +
			"   and k.CONSTRAINT_NAME is null";
	private TapConnectionContext tapConnectionContext;
	private MysqlJdbcContext mysqlJdbcContext;

	public MysqlSchemaLoader(MysqlJdbcContext mysqlJdbcContext) {
		this.mysqlJdbcContext = mysqlJdbcContext;
		this.tapConnectionContext = mysqlJdbcContext.getTapConnectionContext();
	}

	public void discoverSchema(Consumer<List<TapTable>> consumer) throws Throwable {
		if (null == consumer) {
			throw new IllegalArgumentException("Consumer cannot be null");
		}
		DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
		String database = connectionConfig.getString("database");
		List<TapTable> tempList = new ArrayList<>();
		mysqlJdbcContext.query(String.format(SELECT_TABLES, database), tableRs -> {
			while (tableRs.next()) {
				TapTable tapTable = TapSimplify.table(tableRs.getString("TABLE_NAME"));
				try {
					addColumns(tapConnectionContext, tapTable);
				} catch (Exception e) {
					TapLogger.info(TAG, e.getMessage() + "\n" + TapSimplify.getStackString(e));
				}
				tempList.add(tapTable);
				if (tempList.size() == BATCH_SIZE) {
					consumer.accept(tempList);
					tempList.clear();
				}
			}
			if (CollectionUtils.isNotEmpty(tempList)) {
				consumer.accept(tempList);
				tempList.clear();
			}
		});
	}

	private void addColumns(TapConnectionContext connectionContext, TapTable tapTable) throws Exception {
		AtomicInteger primaryPos = new AtomicInteger(1);
		DataMap connectionConfig = connectionContext.getConnectionConfig();
		String database = connectionConfig.getString("database");
		TableFieldTypesGenerator tableFieldTypesGenerator = InstanceFactory.instance(TableFieldTypesGenerator.class);
		try {
			List<String> defaultPk = new ArrayList<>();
			mysqlJdbcContext.query(String.format(SELECT_COLUMNS, database, tapTable.getName()), columnRs -> {
				while (columnRs.next()) {
					String columnName = columnRs.getString("COLUMN_NAME");
					String columnType = columnRs.getString("COLUMN_TYPE");
					TapField field = TapSimplify.field(columnName, columnType);
					tableFieldTypesGenerator.autoFill(field, connectionContext.getSpecification().getDataTypesMap());

					int ordinalPosition = columnRs.getInt("ORDINAL_POSITION");
					field.pos(ordinalPosition);

					String isNullable = columnRs.getString("IS_NULLABLE");
					field.nullable(isNullable.equals("YES"));

					Object columnKey = columnRs.getObject("COLUMN_KEY");
					if (columnKey instanceof String && columnKey.equals("PRI")) {
						field.primaryKeyPos(primaryPos.getAndIncrement());
						defaultPk.add(columnName);
					}

					String columnDefault = columnRs.getString("COLUMN_DEFAULT");
					field.defaultValue(columnDefault);
					tapTable.add(field);
				}
			});
			tapTable.defaultPrimaryKeys(defaultPk);
		} catch (Exception e) {
			throw new Exception("Load column metadata error, table: " + database + "." + tapTable.getName() + "; Reason: " + e.getMessage(), e);
		}
	}
}
