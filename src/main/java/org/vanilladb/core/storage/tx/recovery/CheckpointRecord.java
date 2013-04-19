package org.vanilladb.core.storage.tx.recovery;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.storage.log.BasicLogRecord;


/**
 * The checkpoint log record.
 */
class CheckpointRecord implements LogRecord {

	/**
	 * Creates a quiescent checkpoint record.
	 */
	public CheckpointRecord() {
	}

	/**
	 * Creates a log record by reading no other values from the basic log
	 * record.
	 * 
	 * @param rec
	 *            the basic log record
	 */
	public CheckpointRecord(BasicLogRecord rec) {
	}

	/**
	 * Writes a checkpoint record to the log. This log record contains the
	 * {@link LogRecord#OP_CHECKPOINT} operator ID, and nothing else.
	 * 
	 * @return the LSN of the log record
	 */
	@Override
	public long writeToLog() {
		Constant[] rec = new Constant[] { new IntegerConstant(OP_CHECKPOINT) };
		return logMgr.append(rec);
	}

	@Override
	public int op() {
		return OP_CHECKPOINT;
	}

	/**
	 * Checkpoint records have no associated transaction, and so the method
	 * returns a "dummy", negative txid.
	 */
	@Override
	public long txNumber() {
		return -1; // dummy value
	}

	/**
	 * Does nothing, because a checkpoint record contains no undo information.
	 */
	@Override
	public void undo(long txNum) {
	}

	@Override
	public String toString() {
		return "<CHECKPOINT>";
	}
}
