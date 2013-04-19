package org.vanilladb.core.remote.jdbc;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * The RMI remote interface corresponding to Driver. The method is similar to
 * that of Driver, except that it takes no arguments and throws RemoteExceptions
 * instead of SQLExceptions.
 */
public interface RemoteDriver extends Remote {

	RemoteConnection connect() throws RemoteException;
}
