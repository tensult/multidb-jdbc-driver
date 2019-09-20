package com.tensult.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringUtils;

import com.tensult.jdbc.types.Constants;

public class MultiDatabasesDriver implements Driver {

	private static final String MULTI_CLUSTERS_DRIVER_SCHEME = "jdbc:multidb:";

	private static Logger logger = Logger.getLogger(MultiDatabasesDriver.class.getName());

	private Map<String, Driver> databaseDrivers = new HashMap<String, Driver>();

	private static void registerDriver() {
		try {
			Enumeration<Driver> drivers = DriverManager.getDrivers();
			while (drivers.hasMoreElements()) {
				Driver driver = drivers.nextElement();
				Class<? extends Driver> dirverClazz = driver.getClass();
				if (dirverClazz.isAssignableFrom(MultiDatabasesDriver.class)) {
					return;
				}
			}
			DriverManager.registerDriver(new MultiDatabasesDriver());
		} catch (Exception e) {
			logger.log(Level.SEVERE, e.getMessage());
		}
	}

	public static void main(String[] args) throws SQLException {
		registerDriver();
	}

	static {
		registerDriver();
	}

	public boolean acceptsURL(String url) throws SQLException {
		return StringUtils.startsWith(url, MULTI_CLUSTERS_DRIVER_SCHEME);
	}

	public Connection connectToDatabase(String url, Properties info) throws SQLException {
		Driver dbDriver = getDatabaseDriver(info.getProperty(Constants.DB_DRIVER_CLASS_KEY));
		String dbUrl = getDatabaseUrl(url, info.getProperty(Constants.DB_DRIVER_SCHEME_KEY));
		return dbDriver.connect(dbUrl, info);
	}

	private Driver getDatabaseDriver(String dbDriverClass) throws SQLException {
		try {
			if (!databaseDrivers.containsKey(dbDriverClass)) {
				databaseDrivers.put(dbDriverClass, (Driver) Class.forName(dbDriverClass).newInstance());
			}
			return databaseDrivers.get(dbDriverClass);
		} catch (Throwable e) {
			logger.log(Level.SEVERE, e.getMessage());
			throw new SQLException(e);
		}
	}

	public Connection connect(String url, Properties info) throws SQLException {
		if (info == null || StringUtils.isBlank(info.getProperty(Constants.DB_DRIVER_CLASS_KEY))
				|| StringUtils.isBlank(Constants.DB_DRIVER_SCHEME_KEY)) {
			throw new SQLException(Constants.DB_DRIVER_SCHEME_KEY + "" + " and " + Constants.DB_DRIVER_CLASS_KEY
					+ " should be passed");
		}
		getDatabaseDriver(info.getProperty("DB_DRIVER_CLASS"));
		return new MultiDatabasesConnector(this, url, info);
	}

	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		// TODO: This needs to be implemented
		return new DriverPropertyInfo[0];
	}

	public int getMajorVersion() {
		return 1;
	}

	public int getMinorVersion() {
		return 0;
	}

	public boolean jdbcCompliant() {
		// TODO: This needs to be implemented
		return false;
	}

	private String getDatabaseUrl(String url, String databaseDriverScheme) {
		if (url.startsWith(MULTI_CLUSTERS_DRIVER_SCHEME)) {
			url = databaseDriverScheme + url.substring(MULTI_CLUSTERS_DRIVER_SCHEME.length());
		}
		return url;
	}

	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		return logger;
	}
}