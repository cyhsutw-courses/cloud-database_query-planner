package org.vanilladb.core.storage.tx.recovery;

import org.vanilladb.core.server.VanillaDB;
import org.vanilladb.core.storage.log.LogMgr;


/**
 * The interface to be implemented by each type of log record.
 */
public interface LogRecord {
	/**
	 * @see LogRecord#op()
	 */
	static final int OP_CHECKPOINT = -41, OP_START = -42, OP_COMMIT = -43,
			OP_ROLLBACK = -44;

	static LogMgr logMgr = VanillaDB.logMgr();

	/**
	 * Writes the record to the log and returns its LSN.
	 * 
	 * @return the LSN of the record in the log
	 */
	long writeToLog();

	/**
	 * Returns IDs used to distinguish different logged operations. Depending on
	 * the type of value being set, the operation ID of the
	 * {@link SetValueRecord} equals to the corresponding SQL type. Thus all
	 * other operations cannot have IDs equal to the values defined in
	 * {@link java.sql.Types}.
	 * 
	 * @return the operation ID
	 */
	int op();

	/**
	 * Returns the transaction id stored with the log record.
	 * 
	 * @return the log record's transaction id
	 */
	long txNumber();

	/**
	 * Undoes the operation encoded by this log record. The only log record type
	 * for which this method does anything interesting is {@link SetValueRecord}
	 * .
	 * 
	 * @param txNum
	 *            the id of the transaction that is performing the undo.
	 */
	void undo(long txNum);
}