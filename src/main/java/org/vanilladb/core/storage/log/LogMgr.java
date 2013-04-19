package org.vanilladb.core.storage.log;

import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.storage.file.Page.BLOCK_SIZE;


import java.util.*;

import org.vanilladb.core.server.VanillaDB;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.storage.file.*;

/**
 * The low-level log manager. This log manager is responsible for writing log
 * records into a log file. A log record can be any sequence of integer and
 * string values. The log manager does not understand the meaning of these
 * values, which are written and read by the
 * {@link org.vanilladb.core.storage.tx.recovery.RecoveryMgr recovery
 * manager}.
 */
public class LogMgr implements Iterable<BasicLogRecord> {
	/**
	 * The location where the pointer to the last integer in the page is. A
	 * value of 0 means that the pointer is the first value in the page.
	 */
	public static final int LAST_POS = 0;
	public static final String LOG_FILE;

	private Page myPage = new Page();
	private BlockId currentBlk;
	private int currentPos;

	static {
		String prop = System.getProperty(LogMgr.class.getName() + ".LOG_FILE");
		LOG_FILE = (prop == null ? "vanilladb.log" : prop.trim());
	}

	/**
	 * Creates the manager for the specified log file. If the log file does not
	 * yet exist, it is created with an empty first block. This constructor
	 * depends on a {@link FileMgr} object that it gets from the method
	 * {@link VanillaDB#fileMgr()}. That object is created during system
	 * initialization. Thus this constructor cannot be called until
	 * {@link VanillaDB#initFileMgr(String)} is called first.
	 * 
	 */
	public LogMgr() {
		long logsize = VanillaDB.fileMgr().size(LOG_FILE);
		if (logsize == 0)
			appendNewBlock();
		else {
			currentBlk = new BlockId(LOG_FILE, logsize - 1);
			myPage.read(currentBlk);
			currentPos = getLastRecordPosition() + Page.maxSize(INTEGER);
		}
	}

	/**
	 * Ensures that the log records corresponding to the specified LSN has been
	 * written to disk. All earlier log records will also be written to disk.
	 * 
	 * @param lsn
	 *            the LSN of a log record
	 */
	public synchronized void flush(long lsn) {
		if (lsn >= currentLSN())
			flush();
	}

	/**
	 * Returns an iterator for the log records, which will be returned in
	 * reverse order starting with the most recent.
	 * 
	 * @see java.lang.Iterable#iterator()
	 */
	@Override
	public synchronized Iterator<BasicLogRecord> iterator() {
		flush();
		return new LogIterator(currentBlk);
	}

	/**
	 * Appends a log record to the file. The record contains an arbitrary array
	 * of values. The method also writes an integer to the end of each log
	 * record whose value is the offset of the corresponding integer for the
	 * previous log record. These integers allow log records to be read in
	 * reverse order.
	 * 
	 * @param rec
	 *            the list of values
	 * @return the LSN of the log record
	 */
	public synchronized long append(Constant[] rec) {
		// 4 bytes for the integer that points to the previous log record
		int recsize = Page.maxSize(INTEGER);
		for (Constant c : rec)
			recsize += Page.size(c);
		// if the log record doesn't fit, move to the next block
		if (currentPos + recsize >= BLOCK_SIZE) {
			flush();
			appendNewBlock();
		}
		for (Constant c : rec)
			appendVal(c);
		finalizeRecord();
		return currentLSN();
	}

	/**
	 * Adds the specified value to the page at the position denoted by
	 * currentPos. Then increments currentPos by the size of the value.
	 * 
	 * @param val
	 *            the value to be added to the page
	 */
	private void appendVal(Constant val) {
		myPage.setVal(currentPos, val);
		currentPos += Page.size(val);
	}

	/**
	 * Returns the LSN of the most recent log record. As implemented, the LSN is
	 * the block number where the record is stored. Thus every log record in a
	 * block has the same LSN.
	 * 
	 * @return the LSN of the most recent log record
	 */
	private long currentLSN() {
		return currentBlk.number();
	}

	/**
	 * Writes the current page to the log file.
	 */
	private void flush() {
		myPage.write(currentBlk);
	}

	/**
	 * Clear the current page, and append it to the log file.
	 */
	private void appendNewBlock() {
		setLastRecordPosition(0);
		currentPos = Page.maxSize(INTEGER);
		currentBlk = myPage.append(LOG_FILE);
	}

	/**
	 * Sets up a circular chain of pointers to the records in the page. There is
	 * an integer added to the end of each log record whose value is the offset
	 * of the previous log record. The first four bytes of the page contain an
	 * integer whose value is the offset of the integer for the last log record
	 * in the page.
	 */
	private void finalizeRecord() {
		myPage.setVal(currentPos, new IntegerConstant(getLastRecordPosition()));
		setLastRecordPosition(currentPos);
		currentPos += Page.maxSize(INTEGER);
	}

	private int getLastRecordPosition() {
		return (Integer) myPage.getVal(LAST_POS, INTEGER).asJavaVal();
	}

	private void setLastRecordPosition(int pos) {
		myPage.setVal(LAST_POS, new IntegerConstant(pos));
	}
}
