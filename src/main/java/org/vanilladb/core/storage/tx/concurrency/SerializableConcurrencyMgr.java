package org.vanilladb.core.storage.tx.concurrency;

import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;


public class SerializableConcurrencyMgr extends ConcurrencyMgr {

	public SerializableConcurrencyMgr(long txNumber) {
		txNum = txNumber;
	}

	@Override
	public void onTxCommit(Transaction tx) {
		lockTbl.releaseAll(txNum, false);
	}

	@Override
	public void onTxRollback(Transaction tx) {
		lockTbl.releaseAll(txNum, false);
	}

	@Override
	public void onTxEndStatement(Transaction tx) {
		// do nothing
	}

	@Override
	public void sLock(String fileName) {
		lockTbl.sLock(fileName, txNum);
	}

	@Override
	public void sLock(BlockId blk) {
		lockTbl.isLock(blk.fileName(), txNum);
		lockTbl.sLock(blk, txNum);
	}

	@Override
	public void sLock(RecordId rid) {
		lockTbl.isLock(rid.block().fileName(), txNum);
		lockTbl.isLock(rid.block(), txNum);
		lockTbl.sLock(rid, txNum);
	}

	@Override
	public void xLock(String fileName) {
		lockTbl.xLock(fileName, txNum);
	}

	@Override
	public void xLock(BlockId blk) {
		lockTbl.ixLock(blk.fileName(), txNum);
		lockTbl.xLock(blk, txNum);
	}

	@Override
	public void xLock(RecordId rid) {
		lockTbl.ixLock(rid.block().fileName(), txNum);
		lockTbl.ixLock(rid.block(), txNum);
		lockTbl.xLock(rid, txNum);
	}

	@Override
	public void sixLock(String fileName) {
		lockTbl.sixLock(fileName, txNum);
	}

	@Override
	public void sixLock(BlockId blk) {
		lockTbl.isLock(blk.fileName(), txNum);
		lockTbl.ixLock(blk.fileName(), txNum);
		lockTbl.sixLock(blk, txNum);
	}

	@Override
	public void rangeLock(Object obj) {
		lockTbl.sLock(obj, txNum);
	}

	@Override
	public void sLockIndexBlock(BlockId blk) {
		lockTbl.sLock(blk, txNum);
	}

	@Override
	public void xLockIndexBlock(BlockId blk) {
		lockTbl.xLock(blk, txNum);
	}

	@Override
	public void releaseIndexBlocks(BlockId... blks) {
		for (BlockId b : blks) {
			lockTbl.release(b, txNum, LockTable.S_LOCK);
		}
	}

}
