package org.vanilladb.core.remote.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * This class implements all of the methods of the Driver interface, by throwing
 * an exception for each one. Subclasses (such as {@link VanillaDbDriver}) can
 * override those methods that it want to implement.
 */
public abstract class DriverAdapter implements Driver {
	@Override
	public boolean acceptsURL(String url) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public int getMajorVersion() {
		return 0;
	}

	@Override
	public int getMinorVersion() {
		return 0;
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
		return null;
	}

	@Override
	public boolean jdbcCompliant() {
		return false;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		throw new SQLFeatureNotSupportedException(
				"optinal operation not implemented");
	}
}