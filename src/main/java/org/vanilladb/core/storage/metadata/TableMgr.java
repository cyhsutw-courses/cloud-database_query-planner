package org.vanilladb.core.storage.metadata;

import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.VARCHAR;

import java.util.*;

import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.record.*;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The table manager. There are methods to create a table, save the metadata in
 * the catalog, and obtain the metadata of a previously-created table.
 */
public class TableMgr {
	/**
	 * Name of the table catalog.
	 */
	public static final String TCAT = "tblcat";

	/**
	 * A field name of the table catalog.
	 */
	public static final String TCAT_TBLNAME = "tblname",
			TCAT_RECSIZE = "recsize";

	/**
	 * Name of the field catalog.
	 */
	public static final String FCAT = "fldcat";

	/**
	 * A field name of the field catalog.
	 */
	public static final String FCAT_TBLNAME = "tblname",
			FCAT_FLDNAME = "fldname", FCAT_TYPE = "type",
			FCAT_TYPEARG = "typearg", FCAT_OFFES = "offset";

	/**
	 * The maximum number of characters in any tablename or fieldname.
	 * Currently, this value is 16.
	 */
	public static final int MAX_NAME;

	private TableInfo tcatInfo, fcatInfo;

	static {
		String prop = System
				.getProperty(TableMgr.class.getName() + ".MAX_NAME");
		MAX_NAME = (prop == null ? 16 : Integer.parseInt(prop));
	}

	/**
	 * Creates a new catalog manager for the database system. If the database is
	 * new, then the two catalog tables are created.
	 * 
	 * @param isNew
	 *            has the value true if the database is new
	 * @param tx
	 *            the startup transaction
	 */
	public TableMgr(boolean isNew, Transaction tx) {
		Schema tcatSchema = new Schema();
		tcatSchema.addField(TCAT_TBLNAME, VARCHAR(MAX_NAME));
		tcatSchema.addField(TCAT_RECSIZE, INTEGER);
		tcatInfo = new TableInfo(TCAT, tcatSchema);

		Schema fcatSchema = new Schema();
		fcatSchema.addField(FCAT_TBLNAME, VARCHAR(MAX_NAME));
		fcatSchema.addField(FCAT_FLDNAME, VARCHAR(MAX_NAME));
		fcatSchema.addField(FCAT_TYPE, INTEGER);
		fcatSchema.addField(FCAT_TYPEARG, INTEGER);
		fcatSchema.addField(FCAT_OFFES, INTEGER);
		fcatInfo = new TableInfo(FCAT, fcatSchema);

		if (isNew) {
			createTable(TCAT, tcatSchema, tx);
			createTable(FCAT, fcatSchema, tx);
		}
	}

	/**
	 * Creates a new table having the specified name and schema.
	 * 
	 * @param tblName
	 *            the name of the new table
	 * @param sch
	 *            the table's schema
	 * @param tx
	 *            the transaction creating the table
	 */
	public void createTable(String tblName, Schema sch, Transaction tx) {
		TableInfo ti = new TableInfo(tblName, sch);
		// insert one record into tblcat
		RecordFile tcatfile = tcatInfo.open(tx);
		tcatfile.insert();
		tcatfile.setVal(TCAT_TBLNAME, new VarcharConstant(tblName));
		tcatfile.setVal(TCAT_RECSIZE, new IntegerConstant(ti.recordSize()));
		tcatfile.close();

		// insert a record into fldcat for each field
		RecordFile fcatfile = fcatInfo.open(tx);
		for (String fldname : sch.fields()) {
			fcatfile.insert();
			fcatfile.setVal(FCAT_TBLNAME, new VarcharConstant(tblName));
			fcatfile.setVal(FCAT_FLDNAME, new VarcharConstant(fldname));
			fcatfile.setVal(FCAT_TYPE, new IntegerConstant(sch.type(fldname)
					.getSqlType()));
			fcatfile.setVal(FCAT_TYPEARG, new IntegerConstant(sch.type(fldname)
					.getArgument()));
			fcatfile.setVal(FCAT_OFFES, new IntegerConstant(ti.offset(fldname)));
		}
		fcatfile.close();
	}

	/**
	 * Retrieves the metadata for the specified table out of the catalog.
	 * 
	 * @param tblName
	 *            the name of the table
	 * @param tx
	 *            the transaction
	 * @return the table's stored metadata
	 */
	public TableInfo getTableInfo(String tblName, Transaction tx) {
		RecordFile tcatfile = tcatInfo.open(tx);
		tcatfile.beforeFirst();
		int recsize = -1;
		while (tcatfile.next()) {
			String t = (String) tcatfile.getVal(TCAT_TBLNAME).asJavaVal();
			if (t.equals(tblName)) {
				recsize = (Integer) tcatfile.getVal(TCAT_RECSIZE).asJavaVal();
				break;
			}
		}
		tcatfile.close();

		RecordFile fcatfile = fcatInfo.open(tx);
		fcatfile.beforeFirst();
		Schema sch = new Schema();
		Map<String, Integer> offsets = new HashMap<String, Integer>();
		while (fcatfile.next())
			if (((String) fcatfile.getVal(FCAT_TBLNAME).asJavaVal())
					.equals(tblName)) {
				String fldname = (String) fcatfile.getVal(FCAT_FLDNAME)
						.asJavaVal();
				int fldtype = (Integer) fcatfile.getVal(FCAT_TYPE).asJavaVal();
				int fldarg = (Integer) fcatfile.getVal(FCAT_TYPEARG)
						.asJavaVal();
				int offset = (Integer) fcatfile.getVal(FCAT_OFFES).asJavaVal();
				offsets.put(fldname, offset);
				sch.addField(fldname, Type.newInstance(fldtype, fldarg));
			}
		fcatfile.close();
		if (recsize == -1)
			return null;
		return new TableInfo(tblName, sch, offsets, recsize);
	}
}