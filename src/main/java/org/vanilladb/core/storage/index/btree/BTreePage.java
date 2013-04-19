package org.vanilladb.core.storage.index.btree;

import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.storage.file.Page.BLOCK_SIZE;

import org.vanilladb.core.server.VanillaDB;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.buffer.BufferMgr;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.file.Page;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.concurrency.LockAbortException;


/**
 * A page corresponding to a single B-tree block in a file for {@link BTreeDir}
 * or {@link BTreeLeaf}.
 * <p>
 * The content of each B-tree block begins with an integer storing the number of
 * index records in that page, then a series of integer flags, followed by a
 * series of slots holding index records. Index records are sorted in ascending
 * order.
 * </p>
 */
public class BTreePage {
	private TableInfo ti;
	private BlockId blk;
	private Buffer buff;
	private int numFlags;
	private Transaction tx;
	private int slotSize;
	private String dataFileName;
	private BufferMgr bufferMgr = VanillaDB.bufferMgr();

	/**
	 * Opens a page for the specified B-tree block.
	 * 
	 * @param dataFileName
	 *            the data file name
	 * @param blk
	 *            a block ID refers to the B-tree block
	 * @param numFlags
	 *            the number of flags in this b-tree page
	 * @param ti
	 *            the metadata for the particular B-tree file
	 * @param tx
	 *            the calling transaction
	 */
	public BTreePage(String dataFileName, BlockId blk, int numFlags,
			TableInfo ti, Transaction tx) {
		this.dataFileName = dataFileName;
		this.blk = blk;
		this.numFlags = numFlags;
		this.ti = ti;
		this.tx = tx;
		slotSize = ti.recordSize();
		buff = bufferMgr.pin(blk, tx.getTransactionNumber());
	}

	/**
	 * Closes the page by unpinning its buffer.
	 */
	public void close() {
		if (blk != null)
			bufferMgr.unpin(tx.getTransactionNumber(), buff);
		blk = null;
	}

	/**
	 * Returns the i-th flag.
	 * 
	 * @param i
	 *            flag index, starting from 0
	 * @return the i-th flag
	 */
	public long getFlag(int i) {
		return (Long) buff.getVal(
				Page.maxSize(INTEGER) + Page.maxSize(BIGINT) * i, BIGINT)
				.asJavaVal();
	}

	/**
	 * Sets the i-th flag.
	 * 
	 * @param i
	 *            flag index, starting from 0
	 * @param val
	 *            the flag value
	 * @return the i-th flag
	 */
	public void setFlag(int i, long val) {
		xLockCurrentBlk();
		Constant v = new BigIntConstant(val);
		int offset = Page.maxSize(INTEGER) + Page.maxSize(BIGINT) * i;
		long lsn = tx.recoveryMgr().setVal(buff, offset, v);
		buff.setVal(offset, v, tx.getTransactionNumber(), lsn);
	}

	public Constant getVal(int slot, String fldName) {
		Type type = ti.schema().type(fldName);
		return buff.getVal(fieldPosition(slot, fldName), type);
	}

	public void setVal(int slot, String fldName, Constant val) {
		xLockCurrentBlk();
		Type type = ti.schema().type(fldName);
		Constant v = val.castTo(type);
		int pos = fieldPosition(slot, fldName);
		long lsn = tx.recoveryMgr().setVal(buff, pos, v);
		buff.setVal(pos, v, tx.getTransactionNumber(), lsn);
	}

	public void insert(int slot) {
		for (int i = getNumRecords(); i > slot; i--)
			copyRecord(i - 1, i);
		setNumRecords(getNumRecords() + 1);
	}

	public void delete(int slot) {
		for (int i = slot + 1; i < getNumRecords(); i++)
			copyRecord(i, i - 1);
		setNumRecords(getNumRecords() - 1);
		return;
	}

	/**
	 * Returns true if the block is full.
	 * 
	 * @return true if the block is full
	 */
	public boolean isFull() {
		return slotPosition(getNumRecords() + 1) >= BLOCK_SIZE;
	}

	/**
	 * Returns true if the block is going to be full after insertion.
	 * 
	 * @return true if the block is going to be full after insertion
	 */
	public boolean isGettingFull() {
		return slotPosition(getNumRecords() + 2) >= BLOCK_SIZE;
	}

	/**
	 * Splits the page at the specified slot. A new page is created, and the
	 * records of the page starting from the split slot are transferred to the
	 * new page.
	 * 
	 * @param splitSlot
	 *            the split position
	 * @param flags
	 *            the flag values
	 * @return the number of the new block
	 */
	public long split(int splitSlot, long[] flags) {
		BlockId newBlk = appendNew(flags);
		BTreePage newPage = new BTreePage(dataFileName, newBlk, flags.length,
				ti, tx);
		transferRecords(splitSlot, newPage, 0, getNumRecords() - splitSlot);
		newPage.close();
		return newBlk.number();
	}

	public void transferRecords(int start, BTreePage dest, int destStart,
			int num) {
		int numToTransfer = Math.min(getNumRecords() - start, num);
		for (int i = 0; i < numToTransfer; i++) {
			dest.insert(destStart + i);
			Schema sch = ti.schema();
			for (String fldname : sch.fields())
				dest.setVal(destStart + i, fldname, getVal(start, fldname));
			delete(start);
		}
	}

	public BlockId currentBlk() {
		return blk;
	}

	/**
	 * Returns the number of index records in this page.
	 * 
	 * @return the number of index records in this page
	 */
	public int getNumRecords() {
		return (Integer) buff.getVal(0, INTEGER).asJavaVal();
	}

	private void setNumRecords(int n) {
		xLockCurrentBlk();
		Constant v = new IntegerConstant(n);
		long lsn = tx.recoveryMgr().setVal(buff, 0, v);
		buff.setVal(0, v, tx.getTransactionNumber(), lsn);
	}

	private void copyRecord(int from, int to) {
		Schema sch = ti.schema();
		for (String fldname : sch.fields())
			setVal(to, fldname, getVal(from, fldname));
	}

	private BlockId appendNew(long[] flags) {
		// xlock file
		try {
			tx.concurrencyMgr().xLock(ti.fileName());
		} catch (LockAbortException e) {
			tx.rollback();
			throw e;
		}
		Buffer buff = bufferMgr.pinNew(ti.fileName(), new BTPageFormatter(ti,
				flags), tx.getTransactionNumber());
		bufferMgr.unpin(tx.getTransactionNumber(), buff);
		return buff.block();
	}

	private int fieldPosition(int slot, String fldname) {
		int offset = ti.offset(fldname);
		return slotPosition(slot) + offset;
	}

	private int slotPosition(int slot) {
		return Page.maxSize(INTEGER) + Page.maxSize(BIGINT) * numFlags
				+ (slot * slotSize);
	}

	private void xLockCurrentBlk() {
		try {
			tx.concurrencyMgr().xLockIndexBlock(blk);
		} catch (LockAbortException e) {
			tx.rollback();
			throw e;
		}
	}
}
