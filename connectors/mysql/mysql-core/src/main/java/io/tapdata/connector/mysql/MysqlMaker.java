package io.tapdata.connector.mysql;

import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.LinkedHashMap;
import java.util.stream.Collectors;

/**
 * @author samuel
 * @Description
 * @create 2022-05-06 20:25
 **/
public class MysqlMaker implements SqlMaker {

	private static final String TAG = MysqlMaker.class.getSimpleName();
	private static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE `%s`.`%s`(\n%s) %s";
	private static final String MYSQL_TABLE_TEMPLATE = "`%s`.`%s`";
	private static final String MYSQL_FIELD_TEMPLATE = "`%s`";
	private boolean hasAutoIncrement;
	protected static final int DEFAULT_CONSTRAINT_NAME_MAX_LENGTH = 30;

	@Override
	public String[] createTable(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent, String version) throws Throwable {
		TapTable tapTable = tapCreateTableEvent.getTable();
		LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
		DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
		String database = connectionConfig.getString("database");
		// append field
		String fieldSql = nameFieldMap.values().stream().map(this::createTableAppendField).collect(Collectors.joining(",\n"));
		// primary key
		if (CollectionUtils.isNotEmpty(tapTable.getDefaultPrimaryKeys())) {
			fieldSql += ",\n  " + createTableAppendPrimaryKey(tapTable);
		}
		fieldSql += "\n)";
		String tablePropertiesSql = "";
		// table comment
		if (StringUtils.isNotBlank(tapTable.getComment())) {
			tablePropertiesSql += " COMMENT='" + tapTable.getComment() + "'";
		}

		String sql = String.format(CREATE_TABLE_TEMPLATE, database, tapTable.getId(), fieldSql, tablePropertiesSql);
		return new String[]{sql};
	}

	protected String createTableAppendField(TapField tapField) {
		String fieldSql = "  `" + tapField.getName() + "`"
				+ " " + tapField.getDataType().toUpperCase();

		// auto increment
		// mysql a table can only create one auto-increment column, and must be the primary key
		if (tapField.getAutoInc()
				&& tapField.getPrimaryKeyPos() > 0
				&& !hasAutoIncrement) {
			fieldSql += " AUTO_INCREMENT";
			hasAutoIncrement = true;
		}
		if (tapField.getAutoInc()) {
			if (tapField.getPrimaryKeyPos() == 1) {
				fieldSql += " AUTO_INCREMENT";
			} else {
				TapLogger.warn(TAG, "Field \"{}\" cannot be auto increment in mysql, there can be only one auto column and it must be defined the first key", tapField.getName());
			}
		}

		// nullable
		if (tapField.getNullable()) {
			fieldSql += " NULL";
		} else {
			fieldSql += " NOT NULL";
		}

		// default value
		String defaultValue = tapField.getDefaultValue().toString();
		if (StringUtils.isNotBlank(defaultValue)) {
			fieldSql += " DEFAULT '" + defaultValue + "'";
		}

		// comment
		String comment = tapField.getComment();
		if (StringUtils.isNotBlank(comment)) {
			// try to escape the single quote in comments
			comment = comment.replace("'", "\\'");
			fieldSql += " comment '" + comment + "'";
		}

		return fieldSql;
	}

	protected String createTableAppendPrimaryKey(TapTable tapTable) {
		LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
		String pkSql = "";

		// constraint name
		TapField pkConstraintField = nameFieldMap.values().stream().filter(f -> StringUtils.isNotBlank(f.getConstraint())).findFirst().orElse(null);
		if (pkConstraintField != null) {
			pkSql += "constraint " + getConstraintName(pkConstraintField.getConstraint()) + " primary key (";
		} else {
			pkSql += "primary key (";
		}

		// pk fields
		String pkFieldString = nameFieldMap.values().stream().filter(f -> f.getPrimaryKeyPos() > 0)
				.map(f -> "`" + f.getName() + "`")
				.collect(Collectors.joining(","));

		pkSql += pkFieldString + ")";
		return pkSql;
	}

	protected String getConstraintName(String constraintName) {
		if (StringUtils.isBlank(constraintName)) {
			return "";
		}
		if (constraintName.length() > DEFAULT_CONSTRAINT_NAME_MAX_LENGTH) {
			constraintName = constraintName.substring(0, DEFAULT_CONSTRAINT_NAME_MAX_LENGTH - 4);
		}
		constraintName += RandomStringUtils.randomAlphabetic(4).toUpperCase();
		return constraintName;
	}
}
