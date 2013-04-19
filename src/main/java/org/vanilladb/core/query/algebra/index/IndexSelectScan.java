package org.vanilladb.core.query.algebra.index;

import org.vanilladb.core.query.algebra.*;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.ConstantRange;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.record.RecordId;


/**
 * The scan class corresponding to the select relational algebra operator.
 */
public class IndexSelectScan implements Scan {
	private Index idx;
	private TableScan ts;
	private ConstantRange searchRange;

	/**
	 * Creates an index select scan for the specified index and search range.
	 * 
	 * @param idx
	 *            the index
	 * @param searchRange
	 *            the range of search keys
	 * @param ts
	 *            the table scan of data table
	 */
	public IndexSelectScan(Index idx, ConstantRange searchRange, TableScan ts) {
		this.idx = idx;
		this.searchRange = searchRange;
		this.ts = ts;
		beforeFirst();
	}

	/**
	 * Positions the scan before the first record, which in this case means
	 * positioning the index before the first instance of the selection
	 * constant.
	 * 
	 * @see Scan#beforeFirst()
	 */
	@Override
	public void beforeFirst() {
		idx.beforeFirst(searchRange);
	}

	/**
	 * Moves to the next record, which in this case means moving the index to
	 * the next record satisfying the selection constant, and returning false if
	 * there are no more such index records. If there is a next record, the
	 * method moves the tablescan to the corresponding data record.
	 * 
	 * @see Scan#next()
	 */
	@Override
	public boolean next() {
		boolean ok = idx.next();
		if (ok) {
			RecordId rid = idx.getDataRecordId();
			ts.moveToRecordId(rid);
		}
		return ok;
	}

	/**
	 * Closes the scan by closing the index and the tablescan.
	 * 
	 * @see Scan#close()
	 */
	@Override
	public void close() {
		idx.close();
		ts.close();
	}

	/**
	 * Returns the value of the field of the current data record.
	 * 
	 * @see Scan#getVal(java.lang.String)
	 */
	@Override
	public Constant getVal(String fldName) {
		return ts.getVal(fldName);
	}

	/**
	 * Returns whether the data record has the specified field.
	 * 
	 * @see Scan#hasField(java.lang.String)
	 */
	@Override
	public boolean hasField(String fldName) {
		return ts.hasField(fldName);
	}
}
