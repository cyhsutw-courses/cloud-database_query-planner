package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.record.*;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The Scan class corresponding to a table. A table scan is just a wrapper for a
 * RecordFile object; most methods just delegate to the corresponding RecordFile
 * methods.
 */
public class TableScan implements UpdateScan {
	private RecordFile rf;
	private Schema schema;

	/**
	 * Creates a new table scan, and opens its corresponding record file.
	 * 
	 * @param ti
	 *            the table's metadata
	 * @param tx
	 *            the calling transaction
	 */
	public TableScan(TableInfo ti, Transaction tx) {
		rf = ti.open(tx);
		schema = ti.schema();
	}

	// Scan methods

	@Override
	public void beforeFirst() {
		rf.beforeFirst();
	}

	@Override
	public boolean next() {
		return rf.next();
	}

	@Override
	public void close() {
		rf.close();
	}

	/**
	 * Returns the value of the specified field, as a Constant.
	 * 
	 * @see org.vanilladb.core.query.algebra.algebra.Scan#getVal(java.lang.String)
	 */
	@Override
	public Constant getVal(String fldName) {
		return rf.getVal(fldName);
	}

	@Override
	public boolean hasField(String fldName) {
		return schema.hasField(fldName);
	}

	// UpdateScan methods

	/**
	 * Sets the value of the specified field, as a Constant.
	 * 
	 * @param val
	 *            the constant to be set. Will be casted to the correct type
	 *            specified in the schema of the table.
	 * 
	 * @see UpdateScan#setVal(java.lang.String, Constant)
	 */
	@Override
	public void setVal(String fldName, Constant val) {
		rf.setVal(fldName, val);
	}

	@Override
	public void delete() {
		rf.delete();
	}

	@Override
	public void insert() {
		rf.insert();
	}

	@Override
	public RecordId getRecordId() {
		return rf.currentRecordId();
	}

	@Override
	public void moveToRecordId(RecordId rid) {
		rf.moveToRecordId(rid);
	}
}
