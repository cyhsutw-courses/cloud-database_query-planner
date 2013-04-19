package org.vanilladb.core.remote.jdbc;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.sql.Connection;

import org.vanilladb.core.server.VanillaDB;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The RMI server-side implementation of RemoteConnection.
 */
@SuppressWarnings("serial")
class RemoteConnectionImpl extends UnicastRemoteObject implements
		RemoteConnection {
	private static final int DEFAULT_ISOLATION_LEVEL;
	private Transaction tx;
	private boolean autoCommit = true;
	private boolean readOnly = false;
	private int isolationLevel = DEFAULT_ISOLATION_LEVEL;

	static {
		String prop = System.getProperty(RemoteConnectionImpl.class.getName()
				+ ".DEFAULT_ISOLATION_LEVEL");
		if (prop == null)
			DEFAULT_ISOLATION_LEVEL = Connection.TRANSACTION_SERIALIZABLE;
		else
			DEFAULT_ISOLATION_LEVEL = Integer.parseInt(prop.trim());
	}

	/**
	 * Creates a remote connection and begins a new transaction for it.
	 * 
	 * @throws RemoteException
	 */
	RemoteConnectionImpl() throws RemoteException {
		try {
			tx = VanillaDB.transaction(isolationLevel, readOnly);
		} catch (Exception e) {
			throw new RemoteException("error creating transaction ", e);
		}
	}

	/**
	 * Creates a new RemoteStatement for this connection.
	 * 
	 * @see RemoteConnection#createStatement()
	 */
	@Override
	public RemoteStatement createStatement() throws RemoteException {
		return new RemoteStatementImpl(this);
	}

	/**
	 * Closes the connection. The current transaction is committed.
	 * 
	 * @see RemoteConnection#close()
	 */
	@Override
	public void close() throws RemoteException {
		tx.commit();
	}

	/**
	 * Sets this connection's auto-commit mode to the given state. The default
	 * setting of auto-commit mode is true.
	 */
	@Override
	public void setAutoCommit(boolean autoCommit) throws RemoteException {
		this.autoCommit = autoCommit;
	}

	/**
	 * Sets this connection's auto-commit mode to the given state. The default
	 * setting of auto-commit mode is true. This method may commit current
	 * transaction and start a new transaction.
	 */
	@Override
	public void setReadOnly(boolean readOnly) throws RemoteException {
		if (this.readOnly != readOnly) {
			tx.commit();
			this.readOnly = readOnly;
			try {
				tx = VanillaDB.transaction(isolationLevel, readOnly);
			} catch (Exception e) {
				throw new RemoteException("error creating transaction ", e);
			}
		}
	}

	/**
	 * Attempts to change the transaction isolation level for this connection.
	 * This method will commit current transaction and start a new transaction.
	 * Currently, the only supported isolation levels are
	 * {@link Connection#TRANSACTION_SERIALIZABLE} and
	 * {@link Connection#TRANSACTION_REPEATABLE_READ}. The auto-commit mode must
	 * be set to false before calling this method.
	 */
	@Override
	public void setTransactionIsolation(int level) throws RemoteException {
		if (getAutoCommit() != false)
			throw new RemoteException(
					"the auto-commit mode need to be set to false before changing the isolation level");
		tx.commit();
		this.isolationLevel = level;
		try {
			tx = VanillaDB.transaction(isolationLevel, readOnly);
		} catch (Exception e) {
			throw new RemoteException("error creating transaction ", e);
		}
	}

	/**
	 * Retrieves the current auto-commit mode for this Connection object. *
	 */
	@Override
	public boolean getAutoCommit() throws RemoteException {
		return this.autoCommit;
	}

	/**
	 * Retrieves whether this Connection object is in read-only mode.
	 */
	@Override
	public boolean isReadOnly() throws RemoteException {
		return this.readOnly;
	}

	/**
	 * Retrieves this Connection object's current transaction isolation level.
	 */
	@Override
	public int getTransactionIsolation() throws RemoteException {
		return this.isolationLevel;

	}

	/**
	 * Commits the current transaction, and begins a new one.
	 * 
	 * @throws Exception
	 */
	@Override
	public void commit() throws RemoteException {
		tx.commit();
		try {
			tx = VanillaDB.transaction(isolationLevel, readOnly);
		} catch (Exception e) {
			throw new RemoteException("error creating transaction ", e);
		}
	}

	/**
	 * Rolls back the current transaction, and begins a new one.
	 * 
	 * @throws Exception
	 */
	@Override
	public void rollback() throws RemoteException {
		tx.rollback();
		try {
			tx = VanillaDB.transaction(isolationLevel, readOnly);
		} catch (Exception e) {
			throw new RemoteException("error creating transaction ", e);
		}
	}

	// The following methods are used by the server-side classes.

	/**
	 * Returns the transaction currently associated with this connection.
	 * 
	 * @return the transaction associated with this connection
	 */
	Transaction getTransaction() throws RemoteException {
		return tx;
	}

	/**
	 * Finish the current statement.
	 */
	void endStatement() {
		tx.endStatement();
	}
}
