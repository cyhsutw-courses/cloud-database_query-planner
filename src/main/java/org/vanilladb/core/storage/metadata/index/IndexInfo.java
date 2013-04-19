package org.vanilladb.core.storage.metadata.index;

import static org.vanilladb.core.storage.index.Index.IDX_BTREE;
import static org.vanilladb.core.storage.index.Index.IDX_HASH;

import org.vanilladb.core.server.VanillaDB;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.TableNotFoundException;
import org.vanilladb.core.storage.tx.Transaction;


/**
 * The information about an index. This information is used by the query planner
 * in order to estimate the costs of using the index, and to obtain the schema
 * of the index records. Its methods are essentially the same as those of Plan.
 */
public class IndexInfo {
	private String idxName, fldName, tblName;
	private int idxType;

	/**
	 * Creates an IndexInfo object for the specified index.
	 * 
	 * @param idxName
	 *            the name of the index
	 * @param tblName
	 *            the name of the table
	 * @param fldName
	 *            the name of the indexed field
	 * @param idxType
	 *            the type of the index
	 * @param tx
	 *            the calling transaction
	 */
	public IndexInfo(String idxName, String tblName, String fldName, int idxType) {
		if (idxType != IDX_HASH && idxType != IDX_BTREE)
			throw new IllegalArgumentException();
		this.tblName = tblName;
		this.idxName = idxName;
		this.fldName = fldName;
		this.idxType = idxType;
	}

	/**
	 * Opens the index described by this object.
	 * 
	 * @return the {@link Index} object associated with this information
	 */
	public Index open(Transaction tx) {
		TableInfo ti = VanillaDB.mdMgr().getTableInfo(tblName, tx);
		if (ti == null)
			throw new TableNotFoundException("table '" + tblName
					+ "' is not defined in catalog.");
		return Index.newInstance(tblName + ".tbl", idxName, idxType, ti
				.schema().type(fldName), tx);
	}

	/**
	 * Returns the field name of this IndexInfo
	 * 
	 * @return the name of the indexed filed
	 */
	public String fieldName() {
		return fldName;
	}

	/**
	 * Returns the table name of this IndexInfo
	 * 
	 * @return the name of the indexed table
	 */
	public String tableName() {
		return tblName;
	}

	/**
	 * Returns the type of this IndexInfo
	 * 
	 * @return the type of this index
	 */
	public int indexType() {
		return idxType;
	}

	/**
	 * Returns the name of this index
	 * 
	 * @return the name of this index
	 */
	public String indexName() {
		return idxName;
	}
}
