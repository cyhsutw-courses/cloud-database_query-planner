package org.vanilladb.core.storage.record;

import org.vanilladb.core.server.VanillaDB;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Record;
import org.vanilladb.core.sql.SchemaIncompatibleException;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.buffer.BufferMgr;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.file.FileMgr;
import org.vanilladb.core.storage.file.Page;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.concurrency.LockAbortException;


/**
 * Manages a file of records. There are methods for iterating through the
 * records and accessing their contents.
 * 
 * <p>
 * The {@link #insert()} method must be called before setters.
 * </p>
 * 
 * <p>
 * The {@link #beforeFirst()} method must be called before {@link #next()}.
 * </p>
 */
public class RecordFile implements Record {
	private TableInfo ti;
	private Transaction tx;
	private String fileName;
	private RecordPage rp;
	private long currentBlkNum;
	private BufferMgr bufferMgr = VanillaDB.bufferMgr();
	private FileMgr fileMgr = VanillaDB.fileMgr();

	/**
	 * Constructs an object to manage a file of records. If the file does not
	 * exist, it is created. This method should be called by {@link TableInfo}
	 * only. To obtain an instance of this class, call
	 * {@link TableInfo#open(Transaction)} instead.
	 * 
	 * @param ti
	 *            the table metadata
	 * @param tx
	 *            the transaction
	 */
	public RecordFile(TableInfo ti, Transaction tx) {
		this.ti = ti;
		this.tx = tx;
		fileName = ti.fileName();
	}

	/**
	 * Closes the record file.
	 */
	public void close() {
		if (rp != null)
			rp.close();
	}

	/**
	 * Positions the current record so that a call to method next will wind up
	 * at the first record.
	 */
	public void beforeFirst() {
		close();
		currentBlkNum = -1;
	}

	/**
	 * Moves to the next record. Returns false if there is no next record.
	 * 
	 * @return false if there is no next record.
	 */
	public boolean next() {
		if (fileSize() == 0)
			return false;
		if (currentBlkNum == -1)
			moveTo(0);
		while (true) {
			if (rp.next())
				return true;
			if (atLastBlock())
				return false;
			moveTo(currentBlkNum + 1);
		}
	}

	/**
	 * Returns the value of the specified field in the current record. Getter
	 * should be called after {@link #next()} or {@link #moveToRecordId()}.
	 * 
	 * @param fldName
	 *            the name of the field
	 * 
	 * @return the value at that field
	 */
	public Constant getVal(String fldName) {
		return rp.getVal(fldName);
	}

	/**
	 * Sets a value of the specified field in the current record. The type of
	 * the value must be equal to that of the specified field.
	 * 
	 * @param fldName
	 *            the name of the field
	 * @param val
	 *            the new value for the field
	 */
	public void setVal(String fldName, Constant val) {
		if (tx.isReadOnly() && !isTempTable())
			throw new UnsupportedOperationException();
		Constant v = val.castTo(ti.schema().type(fldName));
		if (Page.size(v) > Page.maxSize(v.getType()))
			throw new SchemaIncompatibleException();
		try {
			tx.concurrencyMgr().xLock(currentRecordId());
		} catch (LockAbortException e) {
			tx.rollback();
			throw e;
		}
		rp.setVal(fldName, v);
	}

	/**
	 * Deletes the current record. The client must call next() to move to the
	 * next record. Calls to methods on a deleted record have unspecified
	 * behavior.
	 */
	public void delete() {
		if (tx.isReadOnly() && !isTempTable())
			throw new UnsupportedOperationException();
		try {
			tx.concurrencyMgr().sixLock(fileName);
			tx.concurrencyMgr().xLock(currentRecordId());
		} catch (LockAbortException e) {
			tx.rollback();
			throw e;
		}
		rp.delete();
	}

	/**
	 * Inserts a new, blank record somewhere in the file beginning at the
	 * current record. If the new record does not fit into an existing block,
	 * then a new block is appended to the file.
	 */
	public void insert() {
		if (tx.isReadOnly() && !isTempTable())
			throw new UnsupportedOperationException();
		try {
			tx.concurrencyMgr().sixLock(fileName);
		} catch (LockAbortException e) {
			tx.rollback();
			throw e;
		}
		if (fileSize() == 0)
			appendBlock();
		moveTo(0);
		// if blank record is found, rp will xlock this record and set the flag
		while (!rp.insert()) {
			if (atLastBlock())
				appendBlock();
			moveTo(currentBlkNum + 1);
		}
	}

	/**
	 * Positions the current record as indicated by the specified record ID .
	 * 
	 * @param rid
	 *            a record ID
	 */
	public void moveToRecordId(RecordId rid) {
		try {
			tx.concurrencyMgr().sLock(rid);
		} catch (LockAbortException e) {
			tx.rollback();
			throw e;
		}
		moveTo(rid.block().number());
		rp.moveToId(rid.id());
	}

	/**
	 * Returns the record ID of the current record.
	 * 
	 * @return a record ID
	 */
	public RecordId currentRecordId() {
		int id = rp.currentId();
		return new RecordId(new BlockId(fileName, currentBlkNum), id);
	}

	/**
	 * Returns the number of blocks in the specified file. This method first
	 * calls corresponding concurrency manager to guarantee the isolation
	 * property, before asking the file manager to return the file size.
	 * 
	 * @return the number of blocks in the file
	 */
	public long fileSize() {
		try {
			tx.concurrencyMgr().rangeLock(fileName);
		} catch (LockAbortException e) {
			tx.rollback();
			throw e;
		}
		return fileMgr.size(fileName);
	}

	private void moveTo(long b) {
		if (rp != null)
			rp.close();
		currentBlkNum = b;
		BlockId blk = new BlockId(fileName, currentBlkNum);
		try {
			tx.concurrencyMgr().sLock(blk);
		} catch (LockAbortException e) {
			tx.rollback();
			throw e;
		}
		rp = new RecordPage(blk, ti, tx, false);
	}

	private boolean atLastBlock() {
		return currentBlkNum == fileSize() - 1;
	}

	private void appendBlock() {
		try {
			tx.concurrencyMgr().xLock(fileName);
		} catch (LockAbortException e) {
			tx.rollback();
			throw e;
		}
		RecordFormatter fmtr = new RecordFormatter(ti);
		Buffer buff = bufferMgr.pinNew(fileName, fmtr,
				tx.getTransactionNumber());
		bufferMgr.unpin(tx.getTransactionNumber(), buff);
	}

	private boolean isTempTable() {
		return fileName.startsWith("_temp");
	}
}