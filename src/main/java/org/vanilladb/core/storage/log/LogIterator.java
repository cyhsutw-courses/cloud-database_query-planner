package org.vanilladb.core.storage.log;

import static org.vanilladb.core.sql.Type.INTEGER;

import java.util.Iterator;

import org.vanilladb.core.storage.file.*;

/**
 * A class that provides the ability to move through the records of the log file
 * in reverse order.
 */
class LogIterator implements Iterator<BasicLogRecord> {
	private BlockId blk;
	private Page pg = new Page();
	private int currentRec;

	/**
	 * Creates an iterator for the records in the log file, positioned after the
	 * last log record. This constructor is called exclusively by
	 * {@link LogMgr#iterator()}.
	 */
	LogIterator(BlockId blk) {
		this.blk = blk;
		pg.read(blk);
		currentRec = (Integer) pg.getVal(LogMgr.LAST_POS, INTEGER).asJavaVal();
	}

	/**
	 * Determines if the current log record is the earliest record in the log
	 * file.
	 * 
	 * @return true if there is an earlier record
	 */
	@Override
	public boolean hasNext() {
		return currentRec > 0 || blk.number() > 0;
	}

	/**
	 * Moves to the next log record in reverse order. If the current log record
	 * is the earliest in its block, then the method moves to the next oldest
	 * block, and returns the log record from there.
	 * 
	 * @return the next earliest log record
	 */
	@Override
	public BasicLogRecord next() {
		if (currentRec == 0)
			moveToNextBlock();
		currentRec = (Integer) pg.getVal(currentRec, INTEGER).asJavaVal();
		return new BasicLogRecord(pg, currentRec + Page.maxSize(INTEGER));
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	/**
	 * Moves to the next log block in reverse order, and positions it after the
	 * last record in that block.
	 */
	private void moveToNextBlock() {
		blk = new BlockId(blk.fileName(), blk.number() - 1);
		pg.read(blk);
		currentRec = (Integer) pg.getVal(LogMgr.LAST_POS, INTEGER).asJavaVal();
	}
}
