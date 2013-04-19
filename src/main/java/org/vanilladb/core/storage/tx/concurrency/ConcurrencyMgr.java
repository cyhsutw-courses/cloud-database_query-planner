package org.vanilladb.core.storage.tx.concurrency;

import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.TransactionLifecycleListener;


/**
 * A locking-based concurrency manager that controls when a {@link Transaction}
 * instance should be stalled to allow concurrency execution of multiple
 * transactions. Each transaction will have its own concurrency manager. This
 * class is intended to be extended to provide different isolation levels.
 */
public abstract class ConcurrencyMgr implements TransactionLifecycleListener {
	protected long txNum;

	protected static LockTable lockTbl = new LockTable();

	public abstract void sLock(String fileName);

	public abstract void sLock(BlockId blk);

	public abstract void sLock(RecordId rid);

	public abstract void xLock(String fileName);

	public abstract void xLock(BlockId blk);

	public abstract void xLock(RecordId rid);

	public abstract void sixLock(String fileName);

	public abstract void sixLock(BlockId blk);

	public abstract void rangeLock(Object obj);

	// For index crabbing

	public abstract void sLockIndexBlock(BlockId blk);

	public abstract void xLockIndexBlock(BlockId blk);

	public abstract void releaseIndexBlocks(BlockId... blks);

}
