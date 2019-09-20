package com.tensult.jdbc;

import java.util.List;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.ObjectUtils;

public class StatementWrapper implements Statement {

	private Integer fetchDirection;
	private Boolean escapeProcessing;
	private Integer maxRows;
	private Integer maxFieldSize = 0;
	private boolean closed = false;
	private Boolean closeOnCompletionStatus = false;
	private String cursorName;
	private Integer queryTimeout;
	private Integer updateCount;
	private boolean poolable;
	private Integer fetchSize;
	private Integer resultSetConcurrency;
	private Integer resultSetType;
	private Integer resultSetHoldability;
	private MultiDatabasesConnector multiDatabasesConnector;
	private Statement recentStatement;
	private Map<String, Statement> databaseStatements = new HashMap<String, Statement>();
	private int batchStatementsCount = 0;
	private final Map<String, List<Integer>> batchStatementConnections = new HashMap<String, List<Integer>>();

	public StatementWrapper(MultiDatabasesConnector multiDatabasesConnector) {
		this.multiDatabasesConnector = multiDatabasesConnector;
	}
	
	public <T> T unwrap(Class<T> iface) throws SQLException {
		try {
			return iface.cast(this);
		} catch (ClassCastException e) {
			throw new SQLException(e.getMessage(), e);
		}
	}

	public boolean isWrapperFor(@SuppressWarnings("rawtypes") Class iface) throws SQLException {
		return iface.isInstance(this);
	}

	private Statement createStatement(Connection connection) throws SQLException {
		Statement newStatement = null;
		if(ObjectUtils.allNotNull(resultSetType, resultSetConcurrency, resultSetHoldability)) {
			newStatement = connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
		} else if(ObjectUtils.allNotNull(resultSetType, resultSetConcurrency)){
			newStatement = connection.createStatement(resultSetType, resultSetConcurrency);
		} else {
			newStatement = connection.createStatement();
		}
		
		if(ObjectUtils.isNotEmpty(cursorName)) {
			newStatement.setCursorName(cursorName);
		}
		if(ObjectUtils.isNotEmpty(escapeProcessing)) {
			newStatement.setEscapeProcessing(escapeProcessing);
		}
		if(ObjectUtils.isNotEmpty(fetchDirection)) {
			newStatement.setFetchDirection(fetchDirection);
		}
		if(ObjectUtils.isNotEmpty(poolable)) {
			newStatement.setPoolable(poolable);
		}
		if(ObjectUtils.isNotEmpty(queryTimeout)) {
			newStatement.setQueryTimeout(queryTimeout);
		}
		if(ObjectUtils.isNotEmpty(maxFieldSize)) {
			newStatement.setMaxFieldSize(maxFieldSize);
		}
		if(BooleanUtils.isTrue(closeOnCompletionStatus)) {
			newStatement.closeOnCompletion();
		}
		if(ObjectUtils.isNotEmpty(fetchSize)) {
			newStatement.setFetchSize(fetchSize);
		}
		return newStatement;
	}

	private Statement getDatabaseStatementForConnection(String connectionId) throws SQLException {
		multiDatabasesConnector.setCurrentConnectionId(connectionId);
		recentStatement = MapUtils.getObject(databaseStatements, connectionId);
		if ((recentStatement == null || recentStatement.isClosed())) {
			Connection existingConnection = multiDatabasesConnector.getCurrentConnection();
			recentStatement = createStatement(existingConnection);

			databaseStatements.put(connectionId, recentStatement);
		}
		return recentStatement;
	}

	private Statement getDatabaseStatement(String sql) throws SQLException {
		String connectionId = multiDatabasesConnector.getQueryConnectionId(sql);
		return getDatabaseStatementForConnection(connectionId);
	}

	public ResultSet executeQuery(String sql) throws SQLException {
		return getDatabaseStatement(sql).executeQuery(sql);
	}

	public boolean execute(String sql) throws SQLException {
		return getDatabaseStatement(sql).execute(sql);
	}

	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
		return getDatabaseStatement(sql).execute(sql, autoGeneratedKeys);
	}

	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		return getDatabaseStatement(sql).execute(sql, columnIndexes);

	}

	public boolean execute(String sql, String[] columnNames) throws SQLException {
		return getDatabaseStatement(sql).execute(sql, columnNames);
	}

	public ResultSet getResultSet() throws SQLException {
		checkClosed();
		return recentStatement.getResultSet();
	}

	public int executeUpdate(String sql) throws SQLException {
		return getDatabaseStatement(sql).executeUpdate(sql);
	}

	public void close() throws SQLException {
		closed = true;
		for (Statement statement : databaseStatements.values()) {
			statement.close();
		}
	}

	public void closeOnCompletion() throws SQLException {
		checkClosed();
		this.closeOnCompletionStatus = true;
	}

	public boolean isCloseOnCompletion() throws SQLException {
		return closeOnCompletionStatus;
	}

	public int getMaxFieldSize() throws SQLException {
		return maxFieldSize;
	}

	public void setMaxFieldSize(int max) throws SQLException {
		checkClosed();
		if (max < 0) {
			throw new SQLException("max should be greater than or equals to 0");
		}
		maxFieldSize = max;
		for (Statement statement : databaseStatements.values()) {
			statement.setMaxFieldSize(max);
		}
	}

	public int getMaxRows() throws SQLException {
		return maxRows;
	}

	public void setMaxRows(int max) throws SQLException {
		checkClosed();
		if (max < 0) {
			throw new SQLException("max should be greater than or equals to 0");
		}
		maxRows = max;
		for (Statement statement : databaseStatements.values()) {
			statement.setMaxRows(maxRows);
		}
	}

	public void setEscapeProcessing(boolean enable) throws SQLException {
		checkClosed();
		escapeProcessing = enable;
		for (Statement statement : databaseStatements.values()) {
			statement.setEscapeProcessing(escapeProcessing);
		}
	}

	public int getQueryTimeout() throws SQLException {
		checkClosed();
		return queryTimeout;
	}

	public void setQueryTimeout(int seconds) throws SQLException {
		checkClosed();
		if (seconds < 0) {
			throw new SQLException("seconds should be greater than or equals to 0");
		}
		queryTimeout = seconds;
		for (Statement statement : databaseStatements.values()) {
			statement.setEscapeProcessing(escapeProcessing);
		}
	}

	public void cancel() throws SQLException {
		checkClosed();
		for (Statement statement : databaseStatements.values()) {
			statement.cancel();
		}
	}

	public SQLWarning getWarnings() throws SQLException {
		checkClosed();
		SQLWarning sqlWarning = new SQLWarning();
		for (Statement statement : databaseStatements.values()) {
			sqlWarning.setNextWarning(statement.getWarnings());
		}
		return sqlWarning;
	}

	public void clearWarnings() throws SQLException {
		checkClosed();
		for (Statement statement : databaseStatements.values()) {
			statement.clearWarnings();
		}
	}

	public void setCursorName(String name) throws SQLException {
		checkClosed();
		cursorName = name;
		for (Statement statement : databaseStatements.values()) {
			statement.setCursorName(cursorName);
		}
	}

	public int getUpdateCount() throws SQLException {
		checkClosed();
		return updateCount;
	}

	public boolean getMoreResults() throws SQLException {
		checkClosed();
		return recentStatement.getMoreResults();
	}

	public void setFetchDirection(int direction) throws SQLException {
		checkClosed();
		fetchDirection = direction;
		for (Statement statement : databaseStatements.values()) {
			statement.setFetchDirection(fetchDirection);
		}
	}

	public int getFetchDirection() throws SQLException {
		checkClosed();
		return fetchDirection;
	}

	public void setFetchSize(int rows) throws SQLException {
		checkClosed();
		if (rows < 0) {
			throw new SQLException("rows should be greater than or equals to 0");
		}
		fetchSize = rows;
		for (Statement statement : databaseStatements.values()) {
			statement.setFetchSize(fetchSize);
		}
	}

	public int getFetchSize() throws SQLException {
		checkClosed();
		return fetchSize;
	}
	
	public void setResultSetConcurrency(int resultSetConcurrency) throws SQLException {
		checkClosed();
		this.resultSetConcurrency = resultSetConcurrency;
	}

	public int getResultSetConcurrency() throws SQLException {
		checkClosed();
		return resultSetConcurrency;
	}
	
	public void setResultSetType(int resultSetType) throws SQLException {
		checkClosed();
		this.resultSetType = resultSetType;
	}

	public int getResultSetType() throws SQLException {
		checkClosed();
		return resultSetType;
	}

	public void addBatch(String sql) throws SQLException {
		checkClosed();
		String connectionId = multiDatabasesConnector.getQueryConnectionId(sql);
		List<Integer> order = MapUtils.getObject(batchStatementConnections, connectionId);
		if (order == null) {
			order = new ArrayList<Integer>();
		}
		order.add(batchStatementsCount);
		batchStatementsCount++;
		batchStatementConnections.put(connectionId, order);
		getDatabaseStatementForConnection(connectionId).addBatch(sql);
	}

	public void clearBatch() throws SQLException {
		checkClosed();
		for (String connectionId : batchStatementConnections.keySet()) {
			getDatabaseStatementForConnection(connectionId).clearBatch();
		}
		batchStatementsCount = 0;
		batchStatementConnections.clear();
	}

	public int[] executeBatch() throws SQLException {
		checkClosed();
		try {
			int[] allConnectionsUpdateCounts = new int[batchStatementsCount];
			for (String connectionId : batchStatementConnections.keySet()) {
				int[] updateCounts = getDatabaseStatementForConnection(connectionId).executeBatch();
				List<Integer> updateCountIndices = batchStatementConnections.get(connectionId);
				for (int i = 0; i < updateCountIndices.size(); i++) {
					allConnectionsUpdateCounts[updateCountIndices.get(i)] = updateCounts[i];
				}
			}
			return allConnectionsUpdateCounts;
		} finally {
			batchStatementsCount = 0;
			batchStatementConnections.clear();
		}
	}

	public Connection getConnection() throws SQLException {
		return multiDatabasesConnector;
	}

	public boolean getMoreResults(int current) throws SQLException {
		return recentStatement.getMoreResults(current);
	}

	public ResultSet getGeneratedKeys() throws SQLException {
		return recentStatement.getGeneratedKeys();
	}

	public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
		checkClosed();
		updateCount = getDatabaseStatement(sql).executeUpdate(sql, autoGeneratedKeys);
		return updateCount;
	}

	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
		checkClosed();
		updateCount = getDatabaseStatement(sql).executeUpdate(sql, columnIndexes);
		return updateCount;

	}

	public int executeUpdate(String sql, String[] columnNames) throws SQLException {
		checkClosed();
		updateCount = getDatabaseStatement(sql).executeUpdate(sql, columnNames);
		return updateCount;
	}

	public void setResultSetHoldability(int resultSetHoldability) throws SQLException {
		checkClosed();
		this.resultSetHoldability = resultSetHoldability;
	}
	
	public int getResultSetHoldability() throws SQLException {
		checkClosed();
		return multiDatabasesConnector.getHoldability();
	}

	public boolean isClosed() throws SQLException {
		return closed;
	}

	public void setPoolable(boolean poolable) throws SQLException {
		checkClosed();
		this.poolable = poolable;
		for (Statement statement : databaseStatements.values()) {
			statement.setPoolable(poolable);
		}
	}

	public boolean isPoolable() throws SQLException {
		checkClosed();
		return poolable;
	}

	void checkClosed() throws SQLException {
		multiDatabasesConnector.checkClosed();
		if (closed) {
			throw new SQLException("Statement is already closed");
		}
	}
}