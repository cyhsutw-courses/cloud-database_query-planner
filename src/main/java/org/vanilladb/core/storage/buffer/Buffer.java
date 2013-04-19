package org.vanilladb.core.storage.buffer;

import java.util.HashSet;
import java.util.Set;

import org.vanilladb.core.server.VanillaDB;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.file.Page;


/**
 * An individual buffer. A buffer wraps a page and stores information about its
 * status, such as the disk block associated with the page, the number of times
 * the block has been pinned, whether the contents of the page have been
 * modified, and if so, the id of the modifying transaction and the LSN of the
 * corresponding log record.
 */
public class Buffer {
	private Page contents = new Page();
	private BlockId blk = null;
	private int pins = 0;
	private Set<Long> modifiedBy = new HashSet<Long>();
	// negative means no corresponding log record
	private long maxLsn = -1;

	/**
	 * Creates a new buffer, wrapping a new {@link Page page}. This constructor
	 * is called exclusively by the class {@link BasicBufferMgr}. It depends on
	 * the {@link org.vanilladb.core.storage.log.LogMgr LogMgr} object
	 * that it gets from the class {@link VanillaDB}. That object is created
	 * during system initialization. Thus this constructor cannot be called
	 * until {@link VanillaDB#initFileAndLogMgr(String)} or is called first.
	 */
	Buffer() {
	}

	/**
	 * Returns the value at the specified offset of this buffer's page. If an
	 * integer was not stored at that location, the behavior of the method is
	 * unpredictable.
	 * 
	 * @param offset
	 *            the byte offset of the page
	 * @param type
	 *            the type of the value
	 * 
	 * @return the constant value at that offset
	 */
	public synchronized Constant getVal(int offset, Type type) {
		return contents.getVal(offset, type);
	}

	/**
	 * Writes a value to the specified offset of this buffer's page. This method
	 * assumes that the transaction has already written an appropriate log
	 * record. The buffer saves the id of the transaction and the LSN of the log
	 * record. A negative lsn value indicates that a log record was not
	 * necessary.
	 * 
	 * @param offset
	 *            the byte offset within the page
	 * @param val
	 *            the new value to be written
	 * @param txNum
	 *            the id of the transaction performing the modification
	 * @param lsn
	 *            the LSN of the corresponding log record
	 */
	public synchronized void setVal(int offset, Constant val, long txNum,
			long lsn) {
		modifiedBy.add(txNum);
		if (lsn >= 0)
			maxLsn = lsn;
		contents.setVal(offset, val);
	}

	/**
	 * Returns a block ID refers to the disk block that the buffer is pinned to.
	 * 
	 * @return a block ID
	 */
	public synchronized BlockId block() {
		return blk;
	}

	/**
	 * Writes the page to its disk block if the page is dirty. The method
	 * ensures that the corresponding log record has been written to disk prior
	 * to writing the page to disk.
	 */
	synchronized void flush() {
		if (modifiedBy.size() > 0) {
			VanillaDB.logMgr().flush(maxLsn);
			contents.write(blk);
			modifiedBy.clear();
		}
	}

	/**
	 * Increases the buffer's pin count.
	 */
	synchronized void pin() {
		pins++;
	}

	/**
	 * Decreases the buffer's pin count.
	 */
	synchronized void unpin() {
		pins--;
	}

	/**
	 * Returns true if the buffer is currently pinned (that is, if it has a
	 * nonzero pin count).
	 * 
	 * @return true if the buffer is pinned
	 */
	synchronized boolean isPinned() {
		return pins > 0;
	}

	/**
	 * Returns true if the buffer is dirty due to a modification by the
	 * specified transaction.
	 * 
	 * @param txNum
	 *            the id of the transaction
	 * @return true if the transaction modified the buffer
	 */
	synchronized boolean isModifiedBy(long txNum) {
		return modifiedBy.contains(txNum);
	}

	/**
	 * Reads the contents of the specified block into the buffer's page. If the
	 * buffer was dirty, then the contents of the previous page are first
	 * written to disk.
	 * 
	 * @param blk
	 *            a block ID
	 */
	synchronized void assignToBlock(BlockId blk) {
		flush();
		this.blk = blk;
		contents.read(blk);
		pins = 0;
	}

	/**
	 * Initializes the buffer's page according to the specified formatter, and
	 * appends the page to the specified file. If the buffer was dirty, then the
	 * contents of the previous page are first written to disk.
	 * 
	 * @param filename
	 *            the name of the file
	 * @param fmtr
	 *            a page formatter, used to initialize the page
	 */
	synchronized void assignToNew(String fileName, PageFormatter fmtr) {
		flush();
		fmtr.format(contents);
		blk = contents.append(fileName);
		pins = 0;
	}
}