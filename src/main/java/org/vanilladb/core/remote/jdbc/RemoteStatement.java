package org.vanilladb.core.remote.jdbc;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * The RMI remote interface corresponding to Statement. The methods are
 * identical to those of Statement, except that they throw RemoteExceptions
 * instead of SQLExceptions.
 */
public interface RemoteStatement extends Remote {

	RemoteResultSet executeQuery(String qry) throws RemoteException;

	int executeUpdate(String cmd) throws RemoteException;
}
