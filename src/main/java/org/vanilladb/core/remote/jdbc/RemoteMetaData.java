package org.vanilladb.core.remote.jdbc;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * The RMI remote interface corresponding to ResultSetMetaData. The methods are
 * identical to those of ResultSetMetaData, except that they throw
 * RemoteExceptions instead of SQLExceptions.
 */
public interface RemoteMetaData extends Remote {

	int getColumnCount() throws RemoteException;

	String getColumnName(int column) throws RemoteException;

	int getColumnType(int column) throws RemoteException;

	int getColumnDisplaySize(int column) throws RemoteException;
}
