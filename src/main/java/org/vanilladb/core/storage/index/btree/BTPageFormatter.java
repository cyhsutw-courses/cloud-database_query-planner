package org.vanilladb.core.storage.index.btree;

import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.storage.file.Page.BLOCK_SIZE;

import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.storage.buffer.PageFormatter;
import org.vanilladb.core.storage.file.Page;
import org.vanilladb.core.storage.metadata.TableInfo;


/**
 * Formats a B-tree page.
 * 
 * @see BTreePage
 */
public class BTPageFormatter implements PageFormatter {
	private TableInfo ti;
	private long[] flags;

	/**
	 * Creates a formatter.
	 * 
	 * @param ti
	 *            the index's metadata
	 * @param flags
	 *            the page's flag values
	 */
	public BTPageFormatter(TableInfo ti, long[] flags) {
		this.ti = ti;
		this.flags = flags;
	}

	/**
	 * Formats the page by initializing as many index-record slots as possible
	 * to have default values.
	 * 
	 * @see PageFormatter#format(Page)
	 */
	@Override
	public void format(Page page) {
		// init number of records
		page.setVal(Page.maxSize(INTEGER), new IntegerConstant(0));
		// set flags
		for (int i = 0; i < flags.length; i++)
			page.setVal(Page.maxSize(INTEGER) + Page.maxSize(BIGINT) * i,
					new BigIntConstant(flags[i]));
		int recSize = ti.recordSize();
		for (int pos = Page.maxSize(INTEGER) + Page.maxSize(BIGINT)
				* flags.length; pos + recSize <= BLOCK_SIZE; pos += recSize)
			makeDefaultRecord(page, pos);
	}

	private void makeDefaultRecord(Page page, int pos) {
		for (String fldname : ti.schema().fields()) {
			int offset = ti.offset(fldname);
			page.setVal(pos + offset,
					Constant.defaultInstance(ti.schema().type(fldname)));
		}
	}
}
