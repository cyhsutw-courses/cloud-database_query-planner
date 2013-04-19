package org.vanilladb.core.storage.tx.recovery;

import static org.vanilladb.core.sql.Type.BIGINT;

import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.storage.log.BasicLogRecord;


class StartRecord implements LogRecord {
	private long txNum;

	/**
	 * Creates a new start log record for the specified transaction.
	 * 
	 * @param txNum
	 *            the ID of the specified transaction
	 */
	public StartRecord(long txNum) {
		this.txNum = txNum;
	}

	/**
	 * Creates a log record by reading one other value from the log.
	 * 
	 * @param rec
	 *            the basic log record
	 */
	public StartRecord(BasicLogRecord rec) {
		this.txNum = (Long) rec.nextVal(BIGINT).asJavaVal();
	}

	/**
	 * Writes a start record to the log. This log record contains the
	 * {@link LogRecord#OP_START} operator ID, followed by the transaction ID.
	 * 
	 * @return the LSN of the log record
	 */
	@Override
	public long writeToLog() {
		Constant[] rec = new Constant[] { new IntegerConstant(OP_START),
				new BigIntConstant(txNum) };
		return logMgr.append(rec);
	}

	@Override
	public int op() {
		return OP_START;
	}

	@Override
	public long txNumber() {
		return txNum;
	}

	/**
	 * Does nothing, because a start record contains no undo information.
	 */
	@Override
	public void undo(long txNum) {
	}

	@Override
	public String toString() {
		return "<START " + txNum + ">";
	}
}
