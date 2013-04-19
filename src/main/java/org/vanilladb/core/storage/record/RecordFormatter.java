package org.vanilladb.core.storage.record;

import static org.vanilladb.core.sql.Type.*;
import static org.vanilladb.core.storage.file.Page.*;
import static org.vanilladb.core.storage.record.RecordPage.EMPTY;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.storage.buffer.PageFormatter;
import org.vanilladb.core.storage.file.Page;
import org.vanilladb.core.storage.metadata.TableInfo;


/**
 * An object that can format a page to look like a block of empty records.
 */
public class RecordFormatter implements PageFormatter {
	private TableInfo ti;

	/**
	 * Creates a formatter for a new page of a table.
	 * 
	 * @param ti
	 *            the table's metadata
	 */
	public RecordFormatter(TableInfo ti) {
		this.ti = ti;
	}

	/**
	 * Formats the page by allocating as many record slots as possible, given
	 * the record size. Each record slot is assigned a flag of EMPTY. Each
	 * integer field is given a value of 0, and each string field is given a
	 * value of "".
	 * 
	 * @see org.vanilladb.core.storage.buffer.PageFormatter#format(org.vanilladb.core.storage.file.Page)
	 */
	@Override
	public void format(Page page) {
		int recsize = ti.recordSize() + Page.maxSize(INTEGER);
		Constant emptyFlag = new IntegerConstant(EMPTY);
		for (int pos = 0; pos + recsize <= BLOCK_SIZE; pos += recsize) {
			page.setVal(pos, emptyFlag);
			makeDefaultRecord(page, pos);
		}
	}

	private void makeDefaultRecord(Page page, int pos) {
		for (String fldname : ti.schema().fields()) {
			int offset = ti.offset(fldname);
			page.setVal(pos + Page.maxSize(INTEGER) + offset,
					Constant.defaultInstance(ti.schema().type(fldname)));
		}
	}
}
