package org.vanilladb.core.remote.jdbc;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * The RMI remote interface corresponding to ResultSet. The methods are
 * identical to those of ResultSet, except that they throw RemoteExceptions
 * instead of SQLExceptions.
 */
public interface RemoteResultSet extends Remote {

	boolean next() throws RemoteException;

	int getInt(String fldName) throws RemoteException;

	long getLong(String fldName) throws RemoteException;

	double getDouble(String fldName) throws RemoteException;

	String getString(String fldName) throws RemoteException;

	RemoteMetaData getMetaData() throws RemoteException;

	void close() throws RemoteException;

	void beforeFirst() throws RemoteException;
}
