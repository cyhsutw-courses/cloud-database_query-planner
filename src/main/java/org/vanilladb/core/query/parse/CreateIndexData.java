package org.vanilladb.core.query.parse;

/**
 * The parser for the <em>create index</em> statement.
 */
public class CreateIndexData {
	private String idxName, tblName, fldName;
	private int idxType;

	/**
	 * Saves the index type, table and field names of the specified index.
	 */
	public CreateIndexData(String idxName, String tblName, String fldName,
			int idxType) {
		this.idxName = idxName;
		this.tblName = tblName;
		this.fldName = fldName;
		this.idxType = idxType;
	}

	/**
	 * Returns the name of the index.
	 * 
	 * @return the name of the index
	 */
	public String indexName() {
		return idxName;
	}

	/**
	 * Returns the name of the indexed table.
	 * 
	 * @return the name of the indexed table
	 */
	public String tableName() {
		return tblName;
	}

	/**
	 * Returns the name of the indexed field.
	 * 
	 * @return the name of the indexed field
	 */
	public String fieldName() {
		return fldName;
	}

	/**
	 * Returns the type of the index.
	 * 
	 * @return the type of the index
	 */
	public int indexType() {
		return idxType;
	}
}
