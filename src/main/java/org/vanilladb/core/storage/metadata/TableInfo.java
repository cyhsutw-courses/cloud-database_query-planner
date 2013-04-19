package org.vanilladb.core.storage.metadata;

import java.util.*;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.file.Page;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.tx.Transaction;


/**
 * The metadata about a table and its records.
 */
public class TableInfo {
	private Schema schema;
	private Map<String, Integer> offsets;
	private int recSize;
	private String tblName;

	/**
	 * Creates a TableInfo object, given a table name and schema. The
	 * constructor calculates the physical offset of each field. This
	 * constructor is used when a table is created.
	 * 
	 * @param tblName
	 *            the name of the table
	 * @param schema
	 *            the schema of the table's records
	 */
	public TableInfo(String tblName, Schema schema) {
		this.schema = schema;
		this.tblName = tblName;
		offsets = new HashMap<String, Integer>();
		int pos = 0;
		for (String fldName : schema.fields()) {
			offsets.put(fldName, pos);
			pos += Page.maxSize(schema.type(fldName));
		}
		recSize = pos;
	}

	/**
	 * Creates a TableInfo object from the specified metadata. This constructor
	 * is used when the metadata is retrieved from the catalog.
	 * 
	 * @param tblName
	 *            the name of the table
	 * @param schema
	 *            the schema of the table's records
	 * @param offsets
	 *            the already-calculated offsets of the fields within a record
	 * @param recSize
	 *            the already-calculated length of each record
	 */
	public TableInfo(String tblName, Schema schema,
			Map<String, Integer> offsets, int recSize) {
		this.tblName = tblName;
		this.schema = schema;
		this.offsets = offsets;
		this.recSize = recSize;
	}

	/**
	 * Returns the filename assigned to this table. Currently, the filename is
	 * the table name followed by ".tbl".
	 * 
	 * @return the name of the file assigned to the table
	 */
	public String fileName() {
		return tblName + ".tbl";
	}

	/**
	 * Returns the table name of this TableInfo
	 * 
	 * @return the name of the file assigned to the table
	 */
	public String tableName() {
		return tblName;
	}

	/**
	 * Returns the schema of the table's records
	 * 
	 * @return the table's record schema
	 */
	public Schema schema() {
		return schema;
	}

	/**
	 * Returns the offset of a specified field within a record
	 * 
	 * @param fldName
	 *            the name of the field
	 * @return the offset of that field within a record
	 */
	public int offset(String fldName) {
		return offsets.get(fldName);
	}

	/**
	 * Returns the number of bytes required to store a record in disk.
	 * 
	 * @return the size of a record, in bytes
	 */
	public int recordSize() {
		return recSize;
	}

	/**
	 * Opens the {@link RecordFile} described by this object.
	 * 
	 * @return the {@link RecordFile} object associated with this information
	 */
	public RecordFile open(Transaction tx) {
		return new RecordFile(this, tx);
	}
}