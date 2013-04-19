package org.vanilladb.core.storage.metadata.index;

import static org.vanilladb.core.sql.Type.*;
import static org.vanilladb.core.storage.metadata.TableMgr.MAX_NAME;

import java.util.*;

import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.TableMgr;
import org.vanilladb.core.storage.record.*;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The index manager. The index manager has similar functionalty to the table
 * manager.
 */
public class IndexMgr {
	/**
	 * Name of the index catalog.
	 */
	public static final String ICAT = "idxcat";

	/**
	 * A field name of the index catalog.
	 */
	public static final String ICAT_IDXNAME = "idxname",
			ICAT_TBLNAME = "tblname", ICAT_FLDNAME = "fldname",
			ICAT_IDXTYPE = "idxtype";

	private TableInfo ti;

	/**
	 * Creates the index manager. This constructor is called during system
	 * startup. If the database is new, then the <em>idxcat</em> table is
	 * created.
	 * 
	 * @param isNew
	 *            indicates whether this is a new database
	 * @param tx
	 *            the system startup transaction
	 */
	public IndexMgr(boolean isNew, TableMgr tblMgr, Transaction tx) {
		if (isNew) {
			Schema sch = new Schema();
			sch.addField(ICAT_IDXNAME, VARCHAR(MAX_NAME));
			sch.addField(ICAT_TBLNAME, VARCHAR(MAX_NAME));
			sch.addField(ICAT_FLDNAME, VARCHAR(MAX_NAME));
			sch.addField(ICAT_IDXTYPE, INTEGER);
			tblMgr.createTable(ICAT, sch, tx);
		}
		ti = tblMgr.getTableInfo(ICAT, tx);
	}

	/**
	 * Creates an index of the specified type for the specified field. A unique
	 * ID is assigned to this index, and its information is stored in the idxcat
	 * table.
	 * 
	 * @param idxName
	 *            the name of the index
	 * @param tblName
	 *            the name of the indexed table
	 * @param fldName
	 *            the name of the indexed field
	 * @param idxType
	 *            the index type of the indexed field
	 * @param tx
	 *            the calling transaction
	 */
	public void createIndex(String idxName, String tblName, String fldName,
			int idxType, Transaction tx) {
		RecordFile rf = ti.open(tx);
		rf.insert();
		rf.setVal(ICAT_IDXNAME, new VarcharConstant(idxName));
		rf.setVal(ICAT_TBLNAME, new VarcharConstant(tblName));
		rf.setVal(ICAT_FLDNAME, new VarcharConstant(fldName));
		rf.setVal(ICAT_IDXTYPE, new IntegerConstant(idxType));
		rf.close();
	}

	/**
	 * Returns a map containing the index info for all indexes on the specified
	 * table.
	 * 
	 * @param tblName
	 *            the name of the table
	 * @param tx
	 *            the calling transaction
	 * @return a map of IndexInfo objects, keyed by their field names
	 */
	public Map<String, IndexInfo> getIndexInfo(String tblName, Transaction tx) {
		Map<String, IndexInfo> result = new HashMap<String, IndexInfo>();
		RecordFile rf = ti.open(tx);
		rf.beforeFirst();
		while (rf.next())
			if (((String) rf.getVal(ICAT_TBLNAME).asJavaVal()).equals(tblName)) {
				String idxname = (String) rf.getVal(ICAT_IDXNAME).asJavaVal();
				String fldname = (String) rf.getVal(ICAT_FLDNAME).asJavaVal();
				int idxtype = (Integer) rf.getVal(ICAT_IDXTYPE).asJavaVal();
				IndexInfo ii = new IndexInfo(idxname, tblName, fldname, idxtype);
				result.put(fldname, ii);
			}
		rf.close();
		return result;
	}
}
