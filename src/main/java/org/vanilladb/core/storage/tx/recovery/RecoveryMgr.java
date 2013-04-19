package org.vanilladb.core.storage.tx.recovery;

import static org.vanilladb.core.storage.tx.recovery.LogRecord.*;

import java.util.*;

import org.vanilladb.core.server.VanillaDB;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.TransactionLifecycleListener;

/**
 * The recovery manager. Each transaction has its own recovery manager.
 */
public class RecoveryMgr implements TransactionLifecycleListener {
	private long txNum;

	/**
	 * Creates a recovery manager for the specified transaction.
	 * 
	 * @param txNum
	 *            the ID of the specified transaction
	 */
	public RecoveryMgr(long txNum) {
		this.txNum = txNum;
		new StartRecord(txNum).writeToLog();
	}

	/**
	 * Writes a commit record to the log, and flushes it to disk.
	 */
	@Override
	public void onTxCommit(Transaction tx) {
		VanillaDB.bufferMgr().flushAll(txNum);
		long lsn = new CommitRecord(txNum).writeToLog();
		VanillaDB.logMgr().flush(lsn);
	}

	/**
	 * Writes a rollback record to the log, and flushes it to disk.
	 */
	@Override
	public void onTxRollback(Transaction tx) {
		doRollback();
		VanillaDB.bufferMgr().flushAll(txNum);
		long lsn = new RollbackRecord(txNum).writeToLog();
		VanillaDB.logMgr().flush(lsn);
	}

	@Override
	public void onTxEndStatement(Transaction tx) {
		// do nothing
	}

	/**
	 * 
	 * Goes through the log, rolling back all uncompleted transactions. Flushes
	 * all modified blocks. Finally, writes a quiescent checkpoint record to the
	 * log and flush it. This method should be called only during system
	 * startup, before user transactions begin.
	 */
	public void recover() {
		doRecover();
		VanillaDB.bufferMgr().flushAll(txNum);
		long lsn = new CheckpointRecord().writeToLog();
		VanillaDB.logMgr().flush(lsn);
	}

	/**
	 * Writes a set value record to the log.
	 * 
	 * @param buff
	 *            the buffer containing the page
	 * @param offset
	 *            the offset of the value in the page
	 * @param newVal
	 *            the value to be written
	 * @return the LSN of the log record, or -1 if updates to temporary files
	 */
	public long setVal(Buffer buff, int offset, Constant newVal) {
		BlockId blk = buff.block();
		if (isTempBlock(blk))
			return -1;
		return new SetValueRecord(txNum, blk, offset, buff.getVal(offset,
				newVal.getType())).writeToLog();
	}

	/**
	 * Rolls back the transaction. The method iterates through the log records,
	 * calling undo() for each log record it finds for the transaction, until it
	 * finds the transaction's START record.
	 */
	private void doRollback() {
		Iterator<LogRecord> iter = new LogRecordIterator();
		while (iter.hasNext()) {
			LogRecord rec = iter.next();
			if (rec.txNumber() == txNum) {
				if (rec.op() == OP_START)
					return;
				rec.undo(txNum);
			}
		}
	}

	/**
	 * Does a complete database recovery. The method iterates through the log
	 * records. Whenever it finds a log record for an unfinished transaction, it
	 * calls undo() on that record. The method stops when it encounters a
	 * CHECKPOINT record or the end of the log.
	 */
	private void doRecover() {
		Collection<Long> finishedTxs = new ArrayList<Long>();
		Iterator<LogRecord> iter = new LogRecordIterator();
		while (iter.hasNext()) {
			LogRecord rec = iter.next();
			if (rec.op() == OP_CHECKPOINT)
				return;
			if (rec.op() == OP_COMMIT || rec.op() == OP_ROLLBACK)
				finishedTxs.add(rec.txNumber());
			else if (!finishedTxs.contains(rec.txNumber()))
				rec.undo(txNum);
		}
	}

	/**
	 * Determines whether a block comes from a temporary file or not.
	 */
	private boolean isTempBlock(BlockId blk) {
		return blk.fileName().startsWith("_temp");
	}
}
