package org.vanilladb.core.storage.log;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.file.Page;


/**
 * A log record located at a specified position of a specified page. The method
 * {@link #nextVal} reads the values sequentially but has no idea what values
 * are. Thus the client is responsible for knowing how many values are in the
 * log record, and what their types are.
 */
public class BasicLogRecord {
	private Page pg;
	private int pos;

	/**
	 * A log record located at the specified position of the specified page.
	 * This constructor is called exclusively by {@link LogIterator#next()}.
	 * 
	 * @param pg
	 *            the page containing the log record
	 * @param pos
	 *            the position of the log record
	 */
	public BasicLogRecord(Page pg, int pos) {
		this.pg = pg;
		this.pos = pos;
	}

	/**
	 * Returns the next value of this log record.
	 * 
	 * @return the next value
	 */
	public Constant nextVal(Type type) {
		Constant val = pg.getVal(pos, type);
		pos += Page.size(val);
		return val;
	}
}
