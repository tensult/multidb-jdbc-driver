package com.tensult.jdbc;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import com.tensult.jdbc.types.Constants;
import com.tensult.jdbc.types.MultiDatabasesDriverConfig;
import com.tensult.utils.ConfigUtils;
import com.tensult.utils.ScriptUtils;

public class MultiDatabasesConnector implements Connection {
	
	private MultiDatabasesDriverConfig driverConfig;
	private MultiDatabasesDriver multiDBdriver;
	private String currentConnectionId;
	private boolean closed = false;
	private Map<String, Connection> databaseConnections = new HashMap<String, Connection>();

	private Properties prepareConnectionInfo(String connectionId) {
		Properties connectionProperties = driverConfig.getConnectionInfo(connectionId);
		Properties deafultConnectionProperties = driverConfig.getConnectionInfo(Constants.DEFAULT_CONNECTION_ID);

		Properties mergedProperties = new Properties();

		if(!StringUtils.equalsIgnoreCase(connectionProperties.getProperty(Constants.EXTEND_DEFAULT_CONNECTION_INFO_KEY), 
				Constants.FALSE_KEY)) {
			mergedProperties.putAll(deafultConnectionProperties);
		}
		mergedProperties.putAll(connectionProperties);
		return mergedProperties;
	}
	
	private Connection getDatabaseConnection(String connectionId) throws SQLException {
		Connection existingConnection = MapUtils.getObject(databaseConnections, connectionId);
		if((existingConnection == null || existingConnection.isClosed())) {
			Properties connectionProperties = prepareConnectionInfo(connectionId);
			String connectionUrl = connectionProperties.getProperty(Constants.URL_PROPERTY_KEY);
			existingConnection = multiDBdriver.connectToDatabase(connectionUrl, connectionProperties);
			databaseConnections.put(connectionId, existingConnection);
		}
		return existingConnection;
	}

	
	public Connection getCurrentConnection() throws SQLException {
		return getDatabaseConnection(getCurrentConnectionId());
	}
	
	public String getQueryConnectionId(String sql) {
		Properties context = new Properties();
		context.put(Constants.SQL_QUERY_KEY, sql);
		return (String) ScriptUtils.execute(driverConfig.getConnectionChooserScript(), context);
	}
	
	private void initialize(String defaultConnectionUrl, Properties defaultConnectionProperties) throws SQLException {
		defaultConnectionProperties.put(Constants.URL_PROPERTY_KEY, defaultConnectionUrl);
		driverConfig =  ConfigUtils.getMultiClusterConfig(defaultConnectionProperties.getProperty(
				Constants.MULTI_DATABASES_DRIVER_CONFIG_PATH_KEY));
		driverConfig.putConnectionInfo(Constants.DEFAULT_CONNECTION_ID, defaultConnectionProperties);
		databaseConnections.put(Constants.DEFAULT_CONNECTION_ID, multiDBdriver.connectToDatabase(
        		defaultConnectionUrl, defaultConnectionProperties));
	}

	public MultiDatabasesConnector(MultiDatabasesDriver multiDBdriver, 
			String defaultConnectionUrl, 
			Properties connectionProperties) throws SQLException {
		this.multiDBdriver = multiDBdriver;
		initialize(defaultConnectionUrl, connectionProperties);
	}

	public void setSchema(String schema) throws SQLException {
		getCurrentConnection().setSchema(schema);
	}

	public String getSchema() throws SQLException {
		return getCurrentConnection().getSchema();
	}

	public void abort(Executor executor) throws SQLException {
		getCurrentConnection().abort(executor);
	}

	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
		getCurrentConnection().setNetworkTimeout(executor, milliseconds);
	}

	public int getNetworkTimeout() throws SQLException {
		return getCurrentConnection().getNetworkTimeout();
	}

	// Semi-copied from
	// http://www.java2s.com/Open-Source/Java-Document/Database-JDBC-Connection-Pool/mysql/com/mysql/jdbc/jdbc2/optional/JDBC4PreparedStatementWrapper.java.htm
	public <T> T unwrap(Class<T> iface) throws SQLException {
		try {
			if ("java.sql.Connection".equals(iface.getName()) || "java.sql.Wrapper.class".equals(iface.getName())) {
				return iface.cast(this);
			}

			return getCurrentConnection().unwrap(iface);
		} catch (ClassCastException cce) {
			throw new SQLException("Unable to unwrap to " + iface.toString(), cce);
		}
	}

	public boolean isWrapperFor(@SuppressWarnings("rawtypes") Class iface) throws SQLException {
		if ("java.sql.Connection".equals(iface.getName()) || "java.sql.Wrapper.class".equals(iface.getName())) {
			return true;
		}
		return getCurrentConnection().isWrapperFor(iface);
	}

	public Statement createStatement() throws SQLException {
		return new StatementWrapper(this);
	}

	public PreparedStatement prepareStatement(String sql) throws SQLException {
		setCurrentConnectionId(getQueryConnectionId(sql));
		return getCurrentConnection().prepareStatement(sql);
	}

	public CallableStatement prepareCall(String sql) throws SQLException {
		setCurrentConnectionId(getQueryConnectionId(sql));
		return getCurrentConnection().prepareCall(sql);
	}

	public String nativeSQL(String sql) throws SQLException {
		setCurrentConnectionId(getQueryConnectionId(sql));
		return getCurrentConnection().nativeSQL(sql);
	}

	public void setAutoCommit(boolean autoCommit) throws SQLException {
		getCurrentConnection().setAutoCommit(autoCommit);
	}

	public boolean getAutoCommit() throws SQLException {
		return getCurrentConnection().getAutoCommit();
	}

	public void commit() throws SQLException {
		getCurrentConnection().commit();
	}

	public void rollback() throws SQLException {
		getCurrentConnection().rollback();
	}

	public void close() throws SQLException {
		this.closed = true;
		getCurrentConnection().close();
	}

	public boolean isClosed() throws SQLException {
		return getCurrentConnection().isClosed();
	}

	public DatabaseMetaData getMetaData() throws SQLException {
		return getCurrentConnection().getMetaData();
	}

	public void setReadOnly(boolean readOnly) throws SQLException {
		getCurrentConnection().setReadOnly(readOnly);
	}

	public boolean isReadOnly() throws SQLException {
		return getCurrentConnection().isReadOnly();
	}

	public void setCatalog(String catalog) throws SQLException {
		getCurrentConnection().setCatalog(catalog);
	}

	public String getCatalog() throws SQLException {
		return getCurrentConnection().getCatalog();
	}

	public void setTransactionIsolation(int level) throws SQLException {
		getCurrentConnection().setTransactionIsolation(level);
	}

	public int getTransactionIsolation() throws SQLException {
		return getCurrentConnection().getTransactionIsolation();
	}

	public SQLWarning getWarnings() throws SQLException {
		return getCurrentConnection().getWarnings();
	}

	public void clearWarnings() throws SQLException {
		getCurrentConnection().clearWarnings();
	}

	public StatementWrapper createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		StatementWrapper statement = new StatementWrapper(this);
		statement.setResultSetType(resultSetType);
		statement.setResultSetConcurrency(resultSetConcurrency);
		return statement;
	}
	
	public StatementWrapper createStatement(int resultSetType, 
			int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		StatementWrapper statement = createStatement(resultSetType, resultSetConcurrency);
		statement.setResultSetHoldability(resultSetHoldability);
		return statement;
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
			throws SQLException {
		setCurrentConnectionId(getQueryConnectionId(sql));
		return getCurrentConnection().prepareStatement(sql, resultSetType, resultSetConcurrency);
	}

	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		setCurrentConnectionId(getQueryConnectionId(sql));
		return getCurrentConnection().prepareCall(sql, resultSetType, resultSetConcurrency);
	}

	public Map<String, Class<?>> getTypeMap() throws SQLException {
		return getCurrentConnection().getTypeMap();
	}

	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		getCurrentConnection().setTypeMap(map);
	}

	public void setHoldability(int holdability) throws SQLException {
		getCurrentConnection().setHoldability(holdability);
	}

	public int getHoldability() throws SQLException {
		return getCurrentConnection().getHoldability();
	}

	public Savepoint setSavepoint() throws SQLException {
		return getCurrentConnection().setSavepoint();
	}

	public Savepoint setSavepoint(String name) throws SQLException {
		return getCurrentConnection().setSavepoint(name);
	}

	public void rollback(Savepoint savepoint) throws SQLException {
		getCurrentConnection().rollback(savepoint);
	}

	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		getCurrentConnection().releaseSavepoint(savepoint);
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		setCurrentConnectionId(getQueryConnectionId(sql));
		return getCurrentConnection().prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		setCurrentConnectionId(getQueryConnectionId(sql));
		return getCurrentConnection().prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		setCurrentConnectionId(getQueryConnectionId(sql));
		return getCurrentConnection().prepareStatement(sql, autoGeneratedKeys);
	}

	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		setCurrentConnectionId(getQueryConnectionId(sql));
		return getCurrentConnection().prepareStatement(sql, columnIndexes);
	}

	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		setCurrentConnectionId(getQueryConnectionId(sql));
		return getCurrentConnection().prepareStatement(sql, columnNames);
	}

	public Clob createClob() throws SQLException {
		return getCurrentConnection().createClob();
	}

	public Blob createBlob() throws SQLException {
		return getCurrentConnection().createBlob();
	}

	public NClob createNClob() throws SQLException {
		return getCurrentConnection().createNClob();
	}

	public SQLXML createSQLXML() throws SQLException {
		return getCurrentConnection().createSQLXML();
	}

	public boolean isValid(int timeout) throws SQLException {
		return getCurrentConnection().isValid(timeout);
	}

	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		try {
			getCurrentConnection().setClientInfo(name, value);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		try {
			getCurrentConnection().setClientInfo(properties);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public String getClientInfo(String name) throws SQLException {
		return getCurrentConnection().getClientInfo(name);
	}

	public Properties getClientInfo() throws SQLException {
		return getCurrentConnection().getClientInfo();
	}

	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		return getCurrentConnection().createArrayOf(typeName, elements);
	}

	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		return getCurrentConnection().createStruct(typeName, attributes);
	}

	public String getCurrentConnectionId() {
		return StringUtils.defaultString(currentConnectionId, Constants.DEFAULT_CONNECTION_ID);
	}

	public void setCurrentConnectionId(String currentConnectionId) {
		this.currentConnectionId = currentConnectionId;
	}
	
	protected void checkClosed() throws SQLException {
       if(closed) {
    	   throw new SQLException("Database connection is already closed");
       }
    }
}