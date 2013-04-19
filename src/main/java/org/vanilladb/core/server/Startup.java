package org.vanilladb.core.server;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.remote.jdbc.RemoteDriver;
import org.vanilladb.core.remote.jdbc.RemoteDriverImpl;


public class Startup {
	private static Logger logger = Logger.getLogger(Startup.class.getName());

	public static void main(String args[]) throws Exception {
		if (logger.isLoggable(Level.INFO))
			logger.info("initing...");

		// configure and initialize the database
		VanillaDB.init(args[0]);

		// create a registry specific for the server on the default port
		Registry reg = LocateRegistry.createRegistry(1099);

		// and post the server entry in it
		RemoteDriver d = new RemoteDriverImpl();
		reg.rebind("vanilladb", d);

		if (logger.isLoggable(Level.INFO))
			logger.info("database server ready");
	}
}
