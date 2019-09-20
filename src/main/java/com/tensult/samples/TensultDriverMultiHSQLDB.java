package com.tensult.samples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class TensultDriverMultiHSQLDB {

	private static void printResultSet(ResultSet rs) throws SQLException {
		ResultSetMetaData rsmd = rs.getMetaData();
		int columnsNumber = rsmd.getColumnCount();
		System.out.println("columnsNumber:" + columnsNumber);

		while (rs.next()) {
			for (int i = 1; i <= columnsNumber; i++)
				System.out.print(rs.getString(i) + " ");
			System.out.println();
		}
	}

	public static void main(String[] args) throws SQLException, ClassNotFoundException {
		System.out.println("This driver connects to multiple hsqldbs and provides query routing");

		Class.forName("com.tensult.jdbc.MultiDatabasesDriver");
		String connectionUrl = "jdbc:multidb://localhost:9001/db1";
		Properties connectionProperties = new Properties();
		connectionProperties.put("DB_DRIVER_CLASS", "org.hsqldb.jdbc.JDBCDriver");
		connectionProperties.put("DB_DRIVER_SCHEME", "jdbc:hsqldb:hsql:");
		connectionProperties.put("user", "sa");
		connectionProperties.put("password", "");
		connectionProperties.put("multi_databases_driver_config_path", "path to multi_databases_driver_config");

		Connection conn = DriverManager.getConnection(connectionUrl, connectionProperties);
		ResultSet result = conn.createStatement().executeQuery("select * from interns;");
		printResultSet(result);
		Statement stmt = conn.createStatement();
		result = stmt.executeQuery("select * from employees;");
		printResultSet(result);

	}

}
