package org.vanilladb.core.storage.tx.recovery;

import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.storage.tx.recovery.LogRecord.*;

import java.util.Iterator;

import org.vanilladb.core.server.VanillaDB;
import org.vanilladb.core.storage.log.BasicLogRecord;


/**
 * A class that provides the ability to read records from the log in reverse
 * order. Unlike the similar class
 * {@link org.vanilladb.core.storage.log.LogIterator LogIterator}, this
 * class understands the meaning of the log records.
 */
class LogRecordIterator implements Iterator<LogRecord> {
	private Iterator<BasicLogRecord> iter = VanillaDB.logMgr().iterator();

	@Override
	public boolean hasNext() {
		return iter.hasNext();
	}

	/**
	 * Constructs a log record from the values in the current basic log record.
	 * The method first reads an integer, which denotes the type of the log
	 * record. Based on that type, the method calls the appropriate LogRecord
	 * constructor to read the remaining values.
	 * 
	 * @return the next log record, or null if no more records
	 */
	@Override
	public LogRecord next() {
		BasicLogRecord rec = iter.next();
		int op = (Integer) rec.nextVal(INTEGER).asJavaVal();
		switch (op) {
		case OP_CHECKPOINT:
			return new CheckpointRecord(rec);
		case OP_START:
			return new StartRecord(rec);
		case OP_COMMIT:
			return new CommitRecord(rec);
		case OP_ROLLBACK:
			return new RollbackRecord(rec);
		default:
			return new SetValueRecord(rec, op);
		}
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
}
