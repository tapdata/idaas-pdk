package io.tapdata.connector.mysql;

import com.mysql.cj.jdbc.StatementImpl;
import com.zaxxer.hikari.HikariDataSource;
import io.tapdata.connector.mysql.util.JdbcUtil;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author samuel
 * @Description
 * @create 2022-04-28 16:24
 **/
public class MysqlJdbcContext implements AutoCloseable {

	private static final String TAG = MysqlJdbcContext.class.getSimpleName();
	private TapConnectionContext tapConnectionContext;
	private String jdbcUrl;
	private HikariDataSource hikariDataSource;
	private static final String SELECT_SQL_MODE = "select @@sql_mode";
	private static final String SET_CLIENT_SQL_MODE = "set sql_mode = ?";
	private static final String SELECT_MYSQL_VERSION = "select version() as version";
	public static final String SELECT_TABLE = "SELECT t.* FROM `%s`.`%s` t";
	private static final String SELECT_COUNT = "SELECT count(*) FROM `%s`.`%s` t";
	private static final String CHECK_TABLE_EXISTS_SQL = "SELECT * FROM information_schema.tables WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s'";
	private static final String DROP_TABLE_IF_EXISTS_SQL = "DROP TABLE IF EXISTS `%s`.`%s`";
	private static final String TRUNCATE_TABLE_SQL = "TRUNCATE TABLE `%s`.`%s`";

	private static final Map<String, String> DEFAULT_PROPERTIES = new HashMap<String, String>() {{
		put("rewriteBatchedStatements", "true");
		put("useCursorFetch", "true");
		put("useSSL", "false");
		put("zeroDateTimeBehavior", "convertToNull");
		put("allowPublicKeyRetrieval", "true");
		put("useTimezone", "false");
		// mysql的布尔类型，实际存储是tinyint(1)，该参数控制mysql客户端接收tinyint(1)的数据类型，默认true，接收为布尔类型，false则为数字:1,0
		put("tinyInt1isBit", "false");
	}};

	private static final List<String> ignoreSqlModes = new ArrayList<String>() {{
		add("NO_ZERO_DATE");
	}};

	public MysqlJdbcContext(TapConnectionContext tapConnectionContext) {
		this.tapConnectionContext = tapConnectionContext;
		this.jdbcUrl = jdbcUrl();
		this.hikariDataSource = HikariConnection.getHikariDataSource(tapConnectionContext, jdbcUrl);
	}

	public Connection getConnection() throws SQLException, IllegalArgumentException {
		Connection connection = this.hikariDataSource.getConnection();
		try {
			setIgnoreSqlMode(connection);
		} catch (Throwable ignored) {
		}
		return connection;
	}

	public static void tryCommit(Connection connection) {
		try {
			if (connection != null && connection.isValid(5) && !connection.getAutoCommit()) {
				connection.commit();
			}
		} catch (Throwable ignored) {
		}
	}

	public static void tryRollBack(Connection connection) {
		try {
			if (connection != null && connection.isValid(5) && !connection.getAutoCommit()) {
				connection.rollback();
			}
		} catch (Throwable ignored) {
		}
	}

	private String jdbcUrl() {
		DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
		String type = tapConnectionContext.getSpecification().getId();
		String host = String.valueOf(connectionConfig.get("host"));
		Integer port = Integer.parseInt(connectionConfig.get("port").toString());
		String databaseName = String.valueOf(connectionConfig.get("database"));

		String additionalString = String.valueOf(connectionConfig.get("addtionalString"));
		additionalString = null == additionalString ? "" : additionalString.trim();
		if (additionalString.startsWith("?")) {
			additionalString = additionalString.substring(1);
		}

		Map<String, String> properties = new HashMap<>();
		StringBuilder sbURL = new StringBuilder("jdbc:").append(type).append("://").append(host).append(":").append(port).append("/").append(databaseName);

		if (StringUtils.isNotBlank(additionalString)) {
			String[] additionalStringSplit = additionalString.split("&");
			for (String s : additionalStringSplit) {
				String[] split = s.split("=");
				if (split.length == 2) {
					properties.put(split[0], split[1]);
				}
			}
		}
		for (String defaultKey : DEFAULT_PROPERTIES.keySet()) {
			if (properties.containsKey(defaultKey)) {
				continue;
			}
			properties.put(defaultKey, DEFAULT_PROPERTIES.get(defaultKey));
		}
		String timezone = connectionConfig.getString("timezone");
		if (StringUtils.isNotBlank(timezone)) {
			String serverTimezone = timezone.replace("+", "%2B");
			properties.put("serverTimezone", serverTimezone);
		}
		StringBuilder propertiesString = new StringBuilder();
		properties.forEach((k, v) -> propertiesString.append("&").append(k).append("=").append(v));

		if (propertiesString.length() > 0) {
			additionalString = StringUtils.removeStart(propertiesString.toString(), "&");
			sbURL.append("?").append(additionalString);
		}

		return sbURL.toString();
	}

	private void setIgnoreSqlMode(Connection connection) throws Throwable {
		if (connection == null) {
			return;
		}
		try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(SELECT_SQL_MODE)) {
			if (resultSet.next()) {
				String sqlMode = resultSet.getString(1);
				if (StringUtils.isBlank(sqlMode)) {
					return;
				}
				for (String ignoreSqlMode : ignoreSqlModes) {
					sqlMode = sqlMode.replace("," + ignoreSqlMode, "");
					sqlMode = sqlMode.replace(ignoreSqlMode + ",", "");
				}

				try (PreparedStatement preparedStatement = connection.prepareStatement(SET_CLIENT_SQL_MODE)) {
					preparedStatement.setString(1, sqlMode);
					preparedStatement.execute();
				}
			}
		}
	}

	public String getMysqlVersion() throws Throwable {
		AtomicReference<String> version = new AtomicReference<>();
		query(SELECT_MYSQL_VERSION, resultSet -> {
			if (resultSet.next()) {
				version.set(resultSet.getString("version"));
			}
		});
		return version.get();
	}

	public void query(String sql, ResultSetConsumer resultSetConsumer) throws Throwable {
		TapLogger.debug(TAG, "Execute query, sql: " + sql);
		try (
				Connection connection = getConnection();
				Statement statement = connection.createStatement();
				ResultSet resultSet = statement.executeQuery(sql)
		) {
			if (null != resultSet) {
				resultSetConsumer.accept(resultSet);
			}
		} catch (SQLException e) {
			throw new Exception("Execute query failed, sql: " + sql + ", code: " + e.getSQLState() + "(" + e.getErrorCode() + "), error: " + e.getMessage(), e);
		}
	}

	public void query(PreparedStatement preparedStatement, ResultSetConsumer resultSetConsumer) throws Throwable {
		TapLogger.debug(TAG, "Execute query, sql: " + preparedStatement);
		try (
				ResultSet resultSet = preparedStatement.executeQuery()
		) {
			if (null != resultSet) {
				resultSetConsumer.accept(resultSet);
			}
		} catch (SQLException e) {
			throw new Exception("Execute query failed, sql: " + preparedStatement + ", code: " + e.getSQLState() + "(" + e.getErrorCode() + "), error: " + e.getMessage(), e);
		}
	}

	public void queryWithStream(String sql, ResultSetConsumer resultSetConsumer) throws Throwable {
		TapLogger.debug(TAG, "Execute query with stream, sql: " + sql);
		try (
				Connection connection = getConnection();
				Statement statement = connection.createStatement()
		) {
			if (statement instanceof StatementImpl) {
				((StatementImpl) statement).enableStreamingResults();
			}
			try (
					ResultSet resultSet = statement.executeQuery(sql)
			) {
				if (null != resultSet) {
					resultSetConsumer.accept(resultSet);
				}
			}
		} catch (SQLException e) {
			throw new Exception("Execute steaming query failed, sql: " + sql + ", code: " + e.getSQLState() + "(" + e.getErrorCode() + "), error: " + e.getMessage(), e);
		}
	}

	public void execute(String sql) throws Throwable {
		TapLogger.debug(TAG, "Execute sql: " + sql);
		try (
				Connection connection = getConnection();
				Statement statement = connection.createStatement()
		) {
			statement.execute(sql);
		} catch (SQLException e) {
			throw new Exception("Execute sql failed, sql: " + sql + ", message: " + e.getSQLState() + " " + e.getErrorCode() + " " + e.getMessage(), e);
		}
	}

	public int count(String tableName) throws Throwable {
		DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
		String database = connectionConfig.getString("database");
		AtomicInteger count = new AtomicInteger(0);
		query(String.format(SELECT_COUNT, database, tableName), rs -> {
			if (rs.next()) {
				count.set(rs.getInt(1));
			}
		});
		return count.get();
	}

	public boolean tableExists(String tableName) throws Throwable {
		AtomicBoolean exists = new AtomicBoolean();
		DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
		String database = connectionConfig.getString("database");
		query(String.format(CHECK_TABLE_EXISTS_SQL, database, tableName), rs -> {
			exists.set(rs.next());
		});
		return exists.get();
	}

	public void dropTable(String tableName) throws Throwable {
		DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
		String database = connectionConfig.getString("database");
		String sql = String.format(DROP_TABLE_IF_EXISTS_SQL, database, tableName);
		execute(sql);
	}

	public void clearTable(String tableName) throws Throwable {
		DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
		String database = connectionConfig.getString("database");
		String sql = String.format(TRUNCATE_TABLE_SQL, database, tableName);
		execute(sql);
	}

	@Override
	public void close() throws Exception {
		JdbcUtil.closeQuietly(hikariDataSource);
	}

	public TapConnectionContext getTapConnectionContext() {
		return tapConnectionContext;
	}

	static class HikariConnection {
		public static HikariDataSource getHikariDataSource(TapConnectionContext tapConnectionContext, String jdbcUrl) throws IllegalArgumentException {
			HikariDataSource hikariDataSource;
			if (null == tapConnectionContext) {
				throw new IllegalArgumentException("TapConnectionContext cannot be null");
			}
			hikariDataSource = new HikariDataSource();
			DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
			hikariDataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
			String username = connectionConfig.getString("username");
			String password = connectionConfig.getString("password");
			hikariDataSource.setJdbcUrl(jdbcUrl);
			hikariDataSource.setUsername(username);
			hikariDataSource.setPassword(password);
			hikariDataSource.setMinimumIdle(1);
			hikariDataSource.setMaximumPoolSize(20);
			hikariDataSource.setAutoCommit(false);
			hikariDataSource.setIdleTimeout(60 * 1000L);
			hikariDataSource.setKeepaliveTime(60 * 1000L);
			return hikariDataSource;
		}
	}
}
