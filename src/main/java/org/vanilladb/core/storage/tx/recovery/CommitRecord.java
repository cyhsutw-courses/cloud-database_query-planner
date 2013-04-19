package org.vanilladb.core.storage.tx.recovery;

import static org.vanilladb.core.sql.Type.BIGINT;

import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.storage.log.BasicLogRecord;


/**
 * The commit log record.
 */
class CommitRecord implements LogRecord {
	private long txNum;

	/**
	 * Creates a new commit log record for the specified transaction.
	 * 
	 * @param txNum
	 *            the ID of the specified transaction
	 */
	public CommitRecord(long txNum) {
		this.txNum = txNum;
	}

	/**
	 * Creates a log record by reading one other value from the log.
	 * 
	 * @param rec
	 *            the basic log record
	 */
	public CommitRecord(BasicLogRecord rec) {
		this.txNum = (Long) rec.nextVal(BIGINT).asJavaVal();
	}

	/**
	 * Writes a commit record to the log. This log record contains the
	 * {@link LogRecord#OP_COMMIT} operator ID, followed by the transaction ID.
	 * 
	 * @return the LSN of the log record
	 */
	@Override
	public long writeToLog() {
		Constant[] rec = new Constant[] { new IntegerConstant(OP_COMMIT),
				new BigIntConstant(txNum) };
		return logMgr.append(rec);
	}

	@Override
	public int op() {
		return OP_COMMIT;
	}

	@Override
	public long txNumber() {
		return txNum;
	}

	/**
	 * Does nothing, because a commit record contains no undo information.
	 */
	@Override
	public void undo(long txNum) {
	}

	@Override
	public String toString() {
		return "<COMMIT " + txNum + ">";
	}
}
