package org.vanilladb.core.storage.record;

import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.storage.file.Page.BLOCK_SIZE;

import org.vanilladb.core.server.VanillaDB;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.buffer.BufferMgr;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.file.Page;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.concurrency.LockAbortException;


/**
 * Manages the placement and access of records in a block.
 */
public class RecordPage {
	public static final int EMPTY = 0, INUSE = 1;

	private BlockId blk;
	private Buffer currentBuff;
	private TableInfo ti;
	private Transaction tx;
	private BufferMgr bufferMgr = VanillaDB.bufferMgr();
	private int slotSize;
	private int currentSlot = -1;
	private boolean controlConcurrency;

	/**
	 * Creates the record manager for the specified block. The current record is
	 * set to be prior to the first one.
	 * 
	 * @param blk
	 *            a block ID
	 * @param ti
	 *            the table's metadata
	 * @param controlConcurrency
	 *            true if the record page needs to control concurrency itself
	 */
	public RecordPage(BlockId blk, TableInfo ti, Transaction tx,
			boolean controlConcurrency) {
		this.blk = blk;
		this.ti = ti;
		this.tx = tx;
		this.controlConcurrency = controlConcurrency;
		slotSize = ti.recordSize() + Page.maxSize(INTEGER);
		currentBuff = bufferMgr.pin(blk, tx.getTransactionNumber());
	}

	/**
	 * Closes the manager, by unpinning the block.
	 */
	public void close() {
		if (blk != null) {
			bufferMgr.unpin(tx.getTransactionNumber(), currentBuff);
			blk = null;
			currentBuff = null;
		}
	}

	/**
	 * Moves to the next record in the block.
	 * 
	 * @return false if there is no next record.
	 */
	public boolean next() {
		return searchFor(INUSE);
	}

	/**
	 * Returns the value stored in the specified field of this record.
	 * 
	 * @param fldName
	 *            the name of the field.
	 * 
	 * @return the constant stored in that field
	 */
	public Constant getVal(String fldName) {
		if (controlConcurrency)
			try {
				tx.concurrencyMgr().sLock(new RecordId(blk, currentSlot));
			} catch (LockAbortException e) {
				tx.rollback();
				throw e;
			}
		Type type = ti.schema().type(fldName);
		int position = fieldPos(fldName);
		return currentBuff.getVal(position, type);
	}

	/**
	 * Stores a value at the specified field of this record.
	 * 
	 * @param fldName
	 *            the name of the field
	 * @param val
	 *            the constant value stored in that field
	 */
	public void setVal(String fldName, Constant val) {
		if (tx.isReadOnly() && !isTempTable())
			throw new UnsupportedOperationException();
		if (controlConcurrency)
			try {
				tx.concurrencyMgr().xLock(new RecordId(blk, currentSlot));
			} catch (LockAbortException e) {
				tx.rollback();
				throw e;
			}
		int position = fieldPos(fldName);
		long lsn = tx.recoveryMgr().setVal(currentBuff, position, val);
		currentBuff.setVal(position, val, tx.getTransactionNumber(), lsn);
	}

	/**
	 * Deletes the current record. Deletion is performed by just marking the
	 * record as "deleted"; the current record does not change. To get to the
	 * next record, call next().
	 * 
	 * @param lsn
	 *            the LSN of the corresponding log record
	 * 
	 */
	public void delete() {
		if (tx.isReadOnly() && !isTempTable())
			throw new UnsupportedOperationException();
		if (controlConcurrency)
			try {
				tx.concurrencyMgr().xLock(new RecordId(blk, currentSlot));
			} catch (LockAbortException e) {
				tx.rollback();
				throw e;
			}
		Constant flag = new IntegerConstant(EMPTY);
		int position = currentPos();
		long lsn = tx.recoveryMgr().setVal(currentBuff, position, flag);
		currentBuff.setVal(currentPos(), flag, tx.getTransactionNumber(), lsn);
	}

	/**
	 * Inserts a new, blank record somewhere in the page. Return false if there
	 * were no available slots.
	 * 
	 * @return false if the insertion was not possible
	 */
	public boolean insert() {
		if (tx.isReadOnly() && !isTempTable())
			throw new UnsupportedOperationException();
		currentSlot = -1;
		boolean found = searchFor(EMPTY);
		if (found) {
			// xlock on this record before setting the flag
			try {
				tx.concurrencyMgr().xLock(new RecordId(blk, currentSlot));
			} catch (LockAbortException e) {
				tx.rollback();
				throw e;
			}
			Constant flag = new IntegerConstant(INUSE);
			int position = currentPos();
			long lsn = tx.recoveryMgr().setVal(currentBuff, position, flag);
			currentBuff.setVal(currentPos(), flag, tx.getTransactionNumber(),
					lsn);
		}
		return found;
	}

	/**
	 * Sets the current record to be the record having the specified ID.
	 * 
	 * @param id
	 *            the ID of the record within the page.
	 */
	public void moveToId(int id) {
		if (controlConcurrency && id == -1) // before first record
			try {
				tx.concurrencyMgr().sLock(blk);
			} catch (LockAbortException e) {
				tx.rollback();
				throw e;
			}
		currentSlot = id;
	}

	/**
	 * Returns the ID of the current record.
	 * 
	 * @return the ID of the current record
	 */
	public int currentId() {
		return currentSlot;
	}

	/**
	 * Returns the BlockId of the current record.
	 * 
	 * @return the BlockId of the current record
	 */
	public BlockId currentBlk() {
		return blk;
	}

	private int currentPos() {
		return currentSlot * slotSize;
	}

	private int fieldPos(String fldName) {
		int offset = Page.maxSize(INTEGER) + ti.offset(fldName);
		return currentPos() + offset;
	}

	private boolean isValidSlot() {
		return currentPos() + slotSize <= BLOCK_SIZE;
	}

	private boolean searchFor(int flag) {
		currentSlot++;
		while (isValidSlot()) {
			int position = currentPos();
			if ((Integer) currentBuff.getVal(position, INTEGER).asJavaVal() == flag)
				return true;
			currentSlot++;
		}
		return false;
	}

	private boolean isTempTable() {
		return ti.fileName().startsWith("_temp");
	}
}
