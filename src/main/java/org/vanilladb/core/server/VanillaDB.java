package org.vanilladb.core.server;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.query.planner.Planner;
import org.vanilladb.core.query.planner.QueryPlanner;
import org.vanilladb.core.query.planner.UpdatePlanner;
import org.vanilladb.core.query.planner.index.IndexUpdatePlanner;
import org.vanilladb.core.query.planner.opt.HeuristicQueryPlanner;
import org.vanilladb.core.storage.buffer.BufferMgr;
import org.vanilladb.core.storage.file.FileMgr;
import org.vanilladb.core.storage.log.LogMgr;
import org.vanilladb.core.storage.metadata.MetadataMgr;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.concurrency.ConcurrencyMgr;
import org.vanilladb.core.storage.tx.concurrency.RepeatableReadConcurrencyMgr;
import org.vanilladb.core.storage.tx.concurrency.SerializableConcurrencyMgr;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;

/**
 * The class that provides system-wide static global values. These values must
 * be initialized by the method {@link #init(String) init} before use. The
 * methods {@link #initFileMgr(String) initFileMgr},
 * {@link #initFileAndLogMgr(String) initFileAndLogMgr},
 * {@link #initFileLogAndBufferMgr(String) initFileLogAndBufferMgr}, and
 * {@link #initMetadataMgr(boolean, Transaction) initMetadataMgr} provide
 * limited initialization, and are useful for debugging purposes.
 */
public class VanillaDB {
	private static Logger logger = Logger.getLogger(VanillaDB.class.getName());
	private static int nextTxNum = 0;

	private static FileMgr fileMgr;
	private static BufferMgr buffMgr;
	private static LogMgr logMgr;
	private static MetadataMgr mdMgr;

	private static boolean inited;
	private static String queryPlanner, updatePlanner, serializableConcurMgr,
			repeatableReadConcurMgr, recMgr;

	/**
	 * Initializes the system. This method is called during system startup.
	 * 
	 * @param dirName
	 *            the name of the database directory
	 */
	public static void init(String dirName) {
		if (inited) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("discarding duplicated init request");
			return;
		}

		// read config file
		boolean config = false;
		String path = System.getProperty("org.vanilladb.core.config.file");
		if (path != null) {
			FileInputStream fis = null;
			try {
				fis = new FileInputStream(path);
				System.getProperties().load(fis);
				config = true;
			} catch (IOException e) {
				// do nothing
			} finally {
				try {
					if (fis != null)
						fis.close();
				} catch (IOException e) {
					// do nothing
				}
			}
		}
		if (!config && logger.isLoggable(Level.WARNING))
			logger.warning("error reading the config file, using defaults");

		String prop = System.getProperty(VanillaDB.class.getName()
				+ ".QUERYPLANNER");
		queryPlanner = (prop == null ? "org.vanilladb.core.query.planner.opt.HeuristicQueryPlanner"
				: prop.trim());
		prop = System.getProperty(VanillaDB.class.getName() + ".UPDATEPLANNER");
		updatePlanner = (prop == null ? "org.vanilladb.core.query.planner.index.IndexUpdatePlanner"
				: prop.trim());
		prop = System.getProperty(VanillaDB.class.getName()
				+ ".SERIALIZABLE_CONCUR_MGR");
		serializableConcurMgr = (prop == null ? "org.vanilladb.core.storage.tx.concurrency.SerializableConcurrencyMgr"
				: prop.trim());
		prop = System.getProperty(VanillaDB.class.getName()
				+ ".REPEATABLE_READ_CONCUR_MGR");
		repeatableReadConcurMgr = (prop == null ? "org.vanilladb.core.storage.tx.concurrency.RepeatableReadConcurrencyMgr"
				: prop.trim());
		prop = System.getProperty(VanillaDB.class.getName() + ".RECOVERY_MGR");
		recMgr = (prop == null ? "org.vanilladb.core.storage.tx.recovery.RecoveryMgr"
				: prop.trim());

		initFileLogAndBufferMgr(dirName);
		Transaction tx = transaction(Connection.TRANSACTION_SERIALIZABLE, false);

		boolean isnew = fileMgr.isNew();
		if (isnew) {
			if (logger.isLoggable(Level.INFO))
				logger.info("creating new database");
		} else {
			if (logger.isLoggable(Level.INFO))
				logger.info("recovering existing database");
			// add a checkpoint record to limit rollback
			tx.recoveryMgr().recover();
		}
		initMetadataMgr(isnew, tx);
		tx.commit();
		inited = true;
	}

	public static boolean isInited() {
		return inited;
	}

	/*
	 * The following initialization methods are useful for testing the
	 * lower-level components of the system without having to initialize
	 * everything.
	 */

	/**
	 * Initializes only the file manager.
	 * 
	 * @param dirName
	 *            the name of the database directory
	 */
	public static void initFileMgr(String dirName) {
		fileMgr = new FileMgr(dirName);
	}

	/**
	 * Initializes the file and log managers.
	 * 
	 * @param dirName
	 *            the name of the database directory
	 */
	public static void initFileAndLogMgr(String dirName) {
		initFileMgr(dirName);
		logMgr = new LogMgr();
	}

	/**
	 * Initializes the file, log, and buffer managers.
	 * 
	 * @param dirName
	 *            the name of the database directory
	 */
	public static void initFileLogAndBufferMgr(String dirName) {
		initFileAndLogMgr(dirName);
		buffMgr = new BufferMgr();
		Transaction.addStartListener(buffMgr);
	}

	/**
	 * Initializes metadata manager.
	 * 
	 * @param isNew
	 *            an indication of whether a new database needs to be created.
	 * @param tx
	 *            the transaction performing the initialization
	 */
	public static void initMetadataMgr(boolean isNew, Transaction tx) {
		mdMgr = new MetadataMgr(isNew, tx);
	}

	public static FileMgr fileMgr() {
		return fileMgr;
	}

	public static BufferMgr bufferMgr() {
		return buffMgr;
	}

	public static LogMgr logMgr() {
		return logMgr;
	}

	public static MetadataMgr mdMgr() {
		return mdMgr;
	}

	/**
	 * Creates a planner for SQL commands. To change how the planner works,
	 * modify this method.
	 * 
	 * @return the system's planner for SQL commands
	 */
	public static Planner planner() {
		QueryPlanner qplanner = null;
		try {
			qplanner = (QueryPlanner) Class.forName(queryPlanner).newInstance();
		} catch (Exception e) {
			// do nothing
		}
		if (qplanner == null) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("no query planner found, using default");
			qplanner = new HeuristicQueryPlanner();
		}

		UpdatePlanner uplanner = null;

		try {
			uplanner = (UpdatePlanner) Class.forName(updatePlanner)
					.newInstance();
		} catch (Exception e) {
			// do nothing
		}

		if (uplanner == null) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("no update planner found, using default");
			uplanner = new IndexUpdatePlanner();
		}
		return new Planner(qplanner, uplanner);
	}

	@SuppressWarnings("rawtypes")
	public static Transaction transaction(int isolationLevel, boolean readOnly) {
		long txNum = nextTxNumber();
		RecoveryMgr recoveryMgr = null;
		try {
			Class<?> cls = Class.forName(recMgr);
			Class partypes[] = new Class[1];
			partypes[0] = Long.TYPE;
			Constructor ct = cls.getConstructor(partypes);
			recoveryMgr = (RecoveryMgr) ct.newInstance(new Long(txNum));
		} catch (Exception e) {
			// do nothing
		}
		if (recoveryMgr == null) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("no recovery mgr found, using default");
			recoveryMgr = new RecoveryMgr(txNum);
		}

		ConcurrencyMgr concurMgr = null;
		switch (isolationLevel) {
		case Connection.TRANSACTION_SERIALIZABLE:
			try {
				Class<?> cls = Class.forName(serializableConcurMgr);
				Class partypes[] = new Class[1];
				partypes[0] = Long.TYPE;
				Constructor ct = cls.getConstructor(partypes);
				concurMgr = (ConcurrencyMgr) ct.newInstance(new Long(txNum));
			} catch (Exception e) {
				System.err.println(e);
			}
			if (concurMgr == null) {
				if (logger.isLoggable(Level.WARNING))
					logger.warning("no serializable concurrency mgr found, using default");
				concurMgr = new SerializableConcurrencyMgr(txNum);
			}
			break;
		case Connection.TRANSACTION_REPEATABLE_READ:
			try {
				Class<?> cls = Class.forName(repeatableReadConcurMgr);
				Class partypes[] = new Class[1];
				partypes[0] = Long.TYPE;
				Constructor ct = cls.getConstructor(partypes);
				concurMgr = (ConcurrencyMgr) ct.newInstance(new Long(txNum));
			} catch (Exception e) {
				// do nothing
			}
			if (concurMgr == null) {
				if (logger.isLoggable(Level.WARNING))
					logger.warning("no repeatable read concurrency mgr found, using default");
				concurMgr = new RepeatableReadConcurrencyMgr(txNum);
			}
			break;
		default:
			throw new UnsupportedOperationException(
					"unsupported isolation level");
		}
		return new Transaction(concurMgr, recoveryMgr, readOnly, txNum);

	}

	private static synchronized long nextTxNumber() {
		nextTxNum++;
		if (logger.isLoggable(Level.FINE))
			logger.fine("new transaction: " + nextTxNum);
		return nextTxNum;
	}
}
