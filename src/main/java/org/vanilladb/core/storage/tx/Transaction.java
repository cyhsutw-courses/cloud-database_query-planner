package org.vanilladb.core.storage.tx;

import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.server.VanillaDB;
import org.vanilladb.core.storage.tx.concurrency.ConcurrencyMgr;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;


/**
 * Provides transaction management for clients, ensuring that all transactions
 * are recoverable, and in general satisfy the ACID properties with specified
 * isolation level.
 */
public class Transaction {
	private static Logger logger = Logger
			.getLogger(Transaction.class.getName());
	private static List<TransactionStartListener> startListeners = new LinkedList<TransactionStartListener>();

	public static void addStartListener(TransactionStartListener listener) {
		startListeners.add(listener);
	}

	private RecoveryMgr recoveryMgr;
	private ConcurrencyMgr concurMgr;
	private List<TransactionLifecycleListener> lifecycleListeners;
	private long txNum;
	private boolean readOnly;

	/**
	 * Creates a new transaction and associates it with a recovery manager and a
	 * concurrency manager. This constructor depends on the file, log, and
	 * buffer managers from {@link VanillaDB}, which are created during system
	 * initialization. Thus this constructor cannot be called until either
	 * {@link VanillaDB#init(String)} or
	 * {@link VanillaDB#initFileLogAndBufferMgr(String)} or is called first.
	 */
	public Transaction(TransactionLifecycleListener concurMgr,
			TransactionLifecycleListener recoveryMgr, boolean readOnly,
			long txNum) {
		this.concurMgr = (ConcurrencyMgr) concurMgr;
		this.recoveryMgr = (RecoveryMgr) recoveryMgr;
		this.txNum = txNum;
		this.readOnly = readOnly;

		lifecycleListeners = new LinkedList<TransactionLifecycleListener>();
		/*
		 * Recover and concurrency managers must be the first and second
		 * handlers respectively.
		 */
		addLifecycleListener(recoveryMgr);
		addLifecycleListener(concurMgr);

		for (TransactionStartListener l : startListeners)
			l.onTxStart(this);
	}

	public void addLifecycleListener(TransactionLifecycleListener listener) {
		lifecycleListeners.add(listener);
	}

	/**
	 * Commits the current transaction. Flushes all modified blocks (and their
	 * log records), writes and flushes a commit record to the log, releases all
	 * locks, and unpins any pinned blocks.
	 */
	public void commit() {
		for (TransactionLifecycleListener l : lifecycleListeners)
			l.onTxCommit(this);

		if (logger.isLoggable(Level.FINE))
			logger.fine("transaction " + txNum + " committed");
	}

	/**
	 * Rolls back the current transaction. Undoes any modified values, flushes
	 * those blocks, writes and flushes a rollback record to the log, releases
	 * all locks, and unpins any pinned blocks.
	 */
	public void rollback() {
		for (TransactionLifecycleListener l : lifecycleListeners)
			l.onTxRollback(this);

		if (logger.isLoggable(Level.FINE))
			logger.fine("transaction " + txNum + " rolled back");
	}

	/**
	 * Finishes the current statement. Releases slocks obtained so far for
	 * repeatable read isolation level and does nothing in serializable
	 * isolation level. This method should be called after each SQL statement.
	 */
	public void endStatement() {
		for (TransactionLifecycleListener l : lifecycleListeners)
			l.onTxEndStatement(this);
	}

	public long getTransactionNumber() {
		return this.txNum;
	}

	public boolean isReadOnly() {
		return this.readOnly;
	}

	public RecoveryMgr recoveryMgr() {
		return recoveryMgr;
	}

	public ConcurrencyMgr concurrencyMgr() {
		return concurMgr;
	}

}
