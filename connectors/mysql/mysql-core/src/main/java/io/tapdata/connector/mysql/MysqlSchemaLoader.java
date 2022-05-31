package io.tapdata.connector.mysql;

import io.tapdata.entity.conversion.TableFieldTypesGenerator;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author samuel
 * @Description
 * @create 2022-04-26 20:09
 **/
public class MysqlSchemaLoader {
	private static final String TAG = MysqlSchemaLoader.class.getSimpleName();
	private static final String SELECT_TABLES = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s' AND TABLE_TYPE='BASE TABLE'";
	private static final String TABLE_NAME_IN = " AND TABLE_NAME IN(%s)";
	private static final String SELECT_COLUMNS = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME='%s'";
	private final static String SELECT_ALL_INDEX_SQL = "select i.TABLE_NAME, i.INDEX_NAME, i.INDEX_TYPE, i.COLLATION, i.NON_UNIQUE, i.COLUMN_NAME, i.SEQ_IN_INDEX from INFORMATION_SCHEMA.STATISTICS i\n" +
			"  left join INFORMATION_SCHEMA.KEY_COLUMN_USAGE k\n" +
			"    on i.TABLE_SCHEMA = k.TABLE_SCHEMA\n" +
			"   and i.TABLE_NAME = k.TABLE_NAME\n" +
			"   and i.INDEX_NAME = CONCAT(k.CONSTRAINT_NAME,'_idx')\n" +
			"   and i.COLUMN_NAME = k.COLUMN_NAME\n" +
			" where i.TABLE_SCHEMA = '%s'\n" +
			"   and i.TABLE_NAME = '%s'\n" +
			"   and i.INDEX_NAME <> 'PRIMARY'\n" +
			"   and k.CONSTRAINT_NAME is null";
	private TapConnectionContext tapConnectionContext;
	private MysqlJdbcContext mysqlJdbcContext;

	public MysqlSchemaLoader(MysqlJdbcContext mysqlJdbcContext) {
		this.mysqlJdbcContext = mysqlJdbcContext;
		this.tapConnectionContext = mysqlJdbcContext.getTapConnectionContext();
	}

	public void discoverSchema(List<String> filterTable, Consumer<List<TapTable>> consumer, int tableSize) throws Throwable {
		if (null == consumer) {
			throw new IllegalArgumentException("Consumer cannot be null");
		}
		DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
		String database = connectionConfig.getString("database");
		List<TapTable> tempList = new ArrayList<>();
		String sql = String.format(SELECT_TABLES, database);
		if (CollectionUtils.isNotEmpty(filterTable)) {
			filterTable = filterTable.stream().map(t -> "'" + t + "'").collect(Collectors.toList());
			String tableNameIn = String.join(",", filterTable);
			sql += String.format(TABLE_NAME_IN, tableNameIn);
		}
		mysqlJdbcContext.query(sql, tableRs -> {
			while (tableRs.next()) {
				TapTable tapTable = TapSimplify.table(tableRs.getString("TABLE_NAME"));
				try {
					discoverFields(tapConnectionContext, tapTable);
				} catch (Exception e) {
					TapLogger.error(TAG, "Discover columns failed, error msg: " + e.getMessage() + "\n" + TapSimplify.getStackString(e));
				}
				try {
					discoverIndexes(tapConnectionContext, tapTable);
				} catch (Throwable e) {
					TapLogger.error(TAG, "Discover indexes failed, error msg: " + e.getMessage() + "\n" + TapSimplify.getStackString(e));
				}
				tempList.add(tapTable);
				if (tempList.size() == tableSize) {
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

	private void discoverFields(TapConnectionContext connectionContext, TapTable tapTable) throws Throwable {
		AtomicInteger primaryPos = new AtomicInteger(1);
		DataMap connectionConfig = connectionContext.getConnectionConfig();
		String database = connectionConfig.getString("database");
		TableFieldTypesGenerator tableFieldTypesGenerator = InstanceFactory.instance(TableFieldTypesGenerator.class);
		try {
			mysqlJdbcContext.query(String.format(SELECT_COLUMNS, database, tapTable.getId()), columnRs -> {
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
					}

					String columnDefault = columnRs.getString("COLUMN_DEFAULT");
					field.defaultValue(columnDefault);
					tapTable.add(field);
				}
			});
		} catch (Exception e) {
			throw new Exception("Load column metadata error, table: " + database + "." + tapTable.getName() + "; Reason: " + e.getMessage(), e);
		}
	}

	private void discoverIndexes(TapConnectionContext tapConnectionContext, TapTable tapTable) throws Throwable {
		DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
		String database = connectionConfig.getString("database");
		List<TapIndex> indexes = new ArrayList<>();
		mysqlJdbcContext.query(String.format(SELECT_ALL_INDEX_SQL, database, tapTable.getId()), indexRs -> {
			while (indexRs.next()) {
				String indexName = indexRs.getString("INDEX_NAME");
				TapIndex tapIndex = indexes.stream().filter(i -> i.getName().equals(indexName)).findFirst().orElse(null);
				if (null == tapIndex) {
					tapIndex = new TapIndex();
					tapIndex.setName(indexName);
					int nonUnique = indexRs.getInt("NON_UNIQUE");
					tapIndex.setUnique(nonUnique == 1);
					tapIndex.setPrimary(false);
					indexes.add(tapIndex);
				}
				List<TapIndexField> indexFields = tapIndex.getIndexFields();
				if (null == indexFields) {
					indexFields = new ArrayList<>();
					tapIndex.setIndexFields(indexFields);
				}
				TapIndexField tapIndexField = new TapIndexField();
				tapIndexField.setName(indexRs.getString("COLUMN_NAME"));
				String collation = indexRs.getString("COLLATION");
				tapIndexField.setFieldAsc("A".equals(collation));
				indexFields.add(tapIndexField);
			}
		});
		tapTable.setIndexList(indexes);
	}
}
