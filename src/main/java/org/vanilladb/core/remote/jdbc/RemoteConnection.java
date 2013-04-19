package org.vanilladb.core.remote.jdbc;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * The RMI remote interface corresponding to Connection. The methods are
 * identical to those of Connection, except that they throw RemoteExceptions
 * instead of SQLExceptions.
 */
public interface RemoteConnection extends Remote {

	RemoteStatement createStatement() throws RemoteException;

	void close() throws RemoteException;

	void setAutoCommit(boolean autoCommit) throws RemoteException;

	void setReadOnly(boolean readOnly) throws RemoteException;

	void setTransactionIsolation(int level) throws RemoteException;

	boolean getAutoCommit() throws RemoteException;

	boolean isReadOnly() throws RemoteException;

	int getTransactionIsolation() throws RemoteException;

	void commit() throws RemoteException;

	void rollback() throws RemoteException;

}
