package org.vanilladb.core.storage.metadata;

import static org.vanilladb.core.sql.Type.VARCHAR;
import static org.vanilladb.core.storage.metadata.TableMgr.MAX_NAME;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.record.*;
import org.vanilladb.core.storage.tx.Transaction;


class ViewMgr {
	/**
	 * Name of the view catalog.
	 */
	public static final String VCAT = "viewcat";

	/**
	 * A field name of the view catalog.
	 */
	public static final String VCAT_VNAME = "viewname", VCAT_VDEF = "viewdef";

	private static final int MAX_VIEWDEF;
	TableMgr tblMgr;

	static {
		String prop = System.getProperty(ViewMgr.class.getName()
				+ ".MAX_VIEWDEF");
		MAX_VIEWDEF = (prop == null ? 100 : Integer.parseInt(prop));
	}

	public ViewMgr(boolean isNew, TableMgr tblMgr, Transaction tx) {
		this.tblMgr = tblMgr;
		if (isNew) {
			Schema sch = new Schema();
			sch.addField(VCAT_VNAME, VARCHAR(MAX_NAME));
			sch.addField(VCAT_VDEF, VARCHAR(MAX_VIEWDEF));
			tblMgr.createTable(VCAT, sch, tx);
		}
	}

	public void createView(String vName, String vDef, Transaction tx) {
		TableInfo ti = tblMgr.getTableInfo(VCAT, tx);
		RecordFile rf = ti.open(tx);
		rf.insert();
		rf.setVal(VCAT_VNAME, new VarcharConstant(vName));
		rf.setVal(VCAT_VDEF, new VarcharConstant(vDef));
		rf.close();
	}

	public String getViewDef(String vName, Transaction tx) {
		String result = null;
		TableInfo ti = tblMgr.getTableInfo(VCAT, tx);
		RecordFile rf = ti.open(tx);
		rf.beforeFirst();
		while (rf.next())
			if (((String) rf.getVal(VCAT_VNAME).asJavaVal()).equals(vName)) {
				result = (String) rf.getVal(VCAT_VDEF).asJavaVal();
				break;
			}
		rf.close();
		return result;
	}
}
