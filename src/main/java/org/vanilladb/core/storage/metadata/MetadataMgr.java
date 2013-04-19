package org.vanilladb.core.storage.metadata;

import java.util.Map;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.metadata.index.IndexMgr;
import org.vanilladb.core.storage.metadata.statistics.StatMgr;
import org.vanilladb.core.storage.metadata.statistics.TableStatInfo;
import org.vanilladb.core.storage.tx.Transaction;


public class MetadataMgr {
	private static TableMgr tblMgr;
	private static ViewMgr viewMgr;
	private static StatMgr statMgr;
	private static IndexMgr idxMgr;

	public MetadataMgr(boolean isNew, Transaction tx) {
		tblMgr = new TableMgr(isNew, tx);
		viewMgr = new ViewMgr(isNew, tblMgr, tx);
		idxMgr = new IndexMgr(isNew, tblMgr, tx);
		statMgr = new StatMgr(tblMgr, tx);
	}

	public void createTable(String tblName, Schema sch, Transaction tx) {
		tblMgr.createTable(tblName, sch, tx);
	}

	public TableInfo getTableInfo(String tblName, Transaction tx) {
		return tblMgr.getTableInfo(tblName, tx);
	}

	public void createView(String viewName, String viewDef, Transaction tx) {
		viewMgr.createView(viewName, viewDef, tx);
	}

	public String getViewDef(String viewName, Transaction tx) {
		return viewMgr.getViewDef(viewName, tx);
	}

	public void createIndex(String idxName, String tblName, String fldName,
			int indexType, Transaction tx) {
		idxMgr.createIndex(idxName, tblName, fldName, indexType, tx);
	}

	public Map<String, IndexInfo> getIndexInfo(String tblName, Transaction tx) {
		return idxMgr.getIndexInfo(tblName, tx);
	}

	public TableStatInfo getTableStatInfo(TableInfo ti, Transaction tx) {
		return statMgr.getTableStatInfo(ti, tx);
	}

	public void countRecordUpdates(String tblName, int count) {
		statMgr.countRecordUpdates(tblName, count);
	}
}
