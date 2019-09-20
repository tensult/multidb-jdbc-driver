package com.tensult.samples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class TensultDriverMultiRedshiftClusters {

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
		System.out.println("This driver connects to multiple redshift cluster and provides query routing");

		Class.forName("com.tensult.jdbc.MultiRedshiftClustersDriver");
		 String connectionUrl = "jdbc:redshiftmulti:iam://redshift-cluster1.ap-south-1.redshift.amazonaws.com:5439/tensult";
		 Properties connectionProperties=new Properties();
		 connectionProperties.put("DB_DRIVER_CLASS", "com.amazon.redshift.jdbc.Driver");
		 connectionProperties.put("DB_DRIVER_SCHEME", "jdbc:redshiftmulti:");
		 connectionProperties.put("plugin_name", "com.amazon.redshift.plugin.AdfsCredentialsProvider");
		 connectionProperties.put("idp_host", "ADFS-host");
		 connectionProperties.put("idp_port", "443");
		 connectionProperties.put("preferred_role", "arn:aws:iam::123456789012:role/Redshift-readonly");
		 connectionProperties.put("user", "AD-domain\\AD-user");
		 connectionProperties.put("password", "AD-password");
		 connectionProperties.put("multi_databases_driver_config_path", "path to multi_databases_driver_config");

		Connection conn = DriverManager.getConnection(connectionUrl, connectionProperties);
		ResultSet result = conn.createStatement().executeQuery("select * from employees where id>1;");
		printResultSet(result);
		Statement stmt = conn.createStatement();
		result = stmt.executeQuery("select * from employees;");

		printResultSet(result);
		
		result = stmt.executeQuery("select * from employees where id>2;");

		printResultSet(result);
		
	}

}
