package org.vanilladb.core.remote.jdbc;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * An adapter class that wraps RemoteConnection. Its methods do nothing except
 * transform RemoteExceptions into SQLExceptions.
 */
public class VanillaDbConnection extends ConnectionAdapter {
	private RemoteConnection rconn;

	public VanillaDbConnection(RemoteConnection c) {
		rconn = c;
	}

	public Statement createStatement() throws SQLException {
		try {
			RemoteStatement rstmt = rconn.createStatement();
			return new VanillaDbStatement(rstmt);
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	public void close() throws SQLException {
		try {
			rconn.close();
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		try {
			rconn.setAutoCommit(autoCommit);
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		try {
			rconn.setReadOnly(readOnly);
		} catch (Exception e) {
			throw new SQLException(e);
		}

	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException {
		try {
			rconn.setTransactionIsolation(level);
		} catch (Exception e) {
			throw new SQLException(e);
		}

	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		try {
			return rconn.getAutoCommit();
		} catch (Exception e) {
			throw new SQLException(e);
		}

	}

	@Override
	public boolean isReadOnly() throws SQLException {
		try {
			return rconn.isReadOnly();
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		try {
			return rconn.getTransactionIsolation();
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	@Override
	public void commit() throws SQLException {
		try {
			rconn.commit();
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	@Override
	public void rollback() throws SQLException {
		try {
			rconn.rollback();
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}
}
