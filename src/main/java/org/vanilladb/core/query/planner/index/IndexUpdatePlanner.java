package org.vanilladb.core.query.planner.index;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.SelectPlan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.algebra.UpdateScan;
import org.vanilladb.core.query.parse.CreateIndexData;
import org.vanilladb.core.query.parse.CreateTableData;
import org.vanilladb.core.query.parse.CreateViewData;
import org.vanilladb.core.query.parse.DeleteData;
import org.vanilladb.core.query.parse.InsertData;
import org.vanilladb.core.query.parse.ModifyData;
import org.vanilladb.core.query.planner.QueryVerifier;
import org.vanilladb.core.query.planner.UpdatePlanner;
import org.vanilladb.core.server.VanillaDB;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * A modification of the basic update planner. It dispatches each update
 * statement to the corresponding index planner.
 */
public class IndexUpdatePlanner implements UpdatePlanner {
	@Override
	public int executeInsert(InsertData data, Transaction tx) {
		QueryVerifier.verifyInsertData(data, tx);
		String tblname = data.tableName();
		Plan p = new TablePlan(tblname, tx);

		// first, insert the record
		UpdateScan s = (UpdateScan) p.open();
		s.insert();
		RecordId rid = s.getRecordId();

		// then modify each field, inserting an index record if appropriate
		Map<String, IndexInfo> indexes = VanillaDB.mdMgr().getIndexInfo(
				tblname, tx);
		Iterator<Constant> valIter = data.vals().iterator();
		for (String fldname : data.fields()) {
			Constant val = valIter.next();
			s.setVal(fldname, val);

			IndexInfo ii = indexes.get(fldname);
			if (ii != null) {
				Index idx = ii.open(tx);
				idx.insert(val, rid);
				idx.close();
			}
		}
		s.close();
		VanillaDB.mdMgr().countRecordUpdates(data.tableName(), 1);
		return 1;
	}

	@Override
	public int executeDelete(DeleteData data, Transaction tx) {
		QueryVerifier.verifyDeleteData(data, tx);
		String tblname = data.tableName();
		Plan p = new TablePlan(tblname, tx);
		p = new SelectPlan(p, data.pred());
		Map<String, IndexInfo> indexes = VanillaDB.mdMgr().getIndexInfo(
				tblname, tx);

		UpdateScan s = (UpdateScan) p.open();
		s.beforeFirst();
		int count = 0;
		while (s.next()) {
			// first, delete the record's record ID from every index
			RecordId rid = s.getRecordId();
			for (String fldname : indexes.keySet()) {
				Constant val = s.getVal(fldname);
				Index idx = indexes.get(fldname).open(tx);
				idx.delete(val, rid);
				idx.close();
			}
			// then delete the record
			s.delete();
			count++;
		}
		s.close();
		VanillaDB.mdMgr().countRecordUpdates(data.tableName(), count);
		return count;
	}

	@Override
	public int executeModify(ModifyData data, Transaction tx) {
		QueryVerifier.verifyModifyData(data, tx);
		String tblname = data.tableName();
		Collection<String> targetflds = data.targetFields();
		Plan p = new TablePlan(tblname, tx);
		p = new SelectPlan(p, data.pred());
		// open all indexs associate with target fields
		HashMap<String, Index> idxMap = new HashMap<String, Index>();
		for (String fld : targetflds) {
			IndexInfo ii = VanillaDB.mdMgr().getIndexInfo(tblname, tx).get(fld);
			Index idx = (ii == null) ? null : ii.open(tx);
			if (idx != null)
				idxMap.put(fld, idx);
		}

		UpdateScan s = (UpdateScan) p.open();
		s.beforeFirst();
		int count = 0;
		while (s.next()) {
			Constant newval, oldval;
			for (String fld : targetflds) {
				// first, update the record
				newval = data.newValue(fld).evaluate(s);
				oldval = s.getVal(fld);
				s.setVal(fld, newval);

				// then update the appropriate index, if it exists
				Index idx = idxMap.get(fld);
				if (idx != null) {
					RecordId rid = s.getRecordId();
					idx.delete(oldval, rid);
					idx.insert(newval, rid);
				}
			}
			count++;
		}
		// close opened indexs
		for (String fld : targetflds) {
			Index idx = idxMap.get(fld);
			if (idx != null)
				idx.close();
		}
		s.close();
		VanillaDB.mdMgr().countRecordUpdates(data.tableName(), count);
		return count;
	}

	@Override
	public int executeCreateTable(CreateTableData data, Transaction tx) {
		QueryVerifier.verifyCreateTableData(data, tx);
		VanillaDB.mdMgr().createTable(data.tableName(), data.newSchema(), tx);
		return 0;
	}

	@Override
	public int executeCreateView(CreateViewData data, Transaction tx) {
		QueryVerifier.verifyCreateViewData(data, tx);
		VanillaDB.mdMgr().createView(data.viewName(), data.viewDef(), tx);
		return 0;
	}

	@Override
	public int executeCreateIndex(CreateIndexData data, Transaction tx) {
		QueryVerifier.verifyCreateIndexData(data, tx);
		VanillaDB.mdMgr().createIndex(data.indexName(), data.tableName(),
				data.fieldName(), data.indexType(), tx);
		return 0;
	}
}
