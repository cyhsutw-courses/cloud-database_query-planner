package org.vanilladb.core.storage.tx.recovery;

import static org.vanilladb.core.sql.Type.BIGINT;

import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.storage.log.BasicLogRecord;


/**
 * The rollback log record.
 */
class RollbackRecord implements LogRecord {
	private long txNum;

	/**
	 * Creates a new rollback log record for the specified transaction.
	 * 
	 * @param txNum
	 *            the ID of the specified transaction
	 */
	public RollbackRecord(long txNum) {
		this.txNum = txNum;
	}

	/**
	 * Creates a log record by reading one other value from the log.
	 * 
	 * @param rec
	 *            the basic log record
	 */
	public RollbackRecord(BasicLogRecord rec) {
		this.txNum = (Long) rec.nextVal(BIGINT).asJavaVal();
	}

	/**
	 * Writes a rollback record to the log. This log record contains the
	 * {@link LogRecord#OP_ROLLBACK} operator ID, followed by the transaction
	 * ID.
	 * 
	 * @return the LSN of the log record
	 */
	@Override
	public long writeToLog() {
		Constant[] rec = new Constant[] { new IntegerConstant(OP_ROLLBACK),
				new BigIntConstant(txNum) };
		return logMgr.append(rec);
	}

	@Override
	public int op() {
		return OP_ROLLBACK;
	}

	@Override
	public long txNumber() {
		return txNum;
	}

	/**
	 * Does nothing, because a rollback record contains no undo information.
	 */
	@Override
	public void undo(long txNum) {
	}

	@Override
	public String toString() {
		return "<ROLLBACK " + txNum + ">";
	}
}
