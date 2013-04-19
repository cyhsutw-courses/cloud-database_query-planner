package org.vanilladb.core.query.planner;

import java.util.Collection;
import java.util.Iterator;

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
import org.vanilladb.core.server.VanillaDB;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The basic planner for SQL update statements.
 * 
 * @author sciore
 */
public class BasicUpdatePlanner implements UpdatePlanner {

	@Override
	public int executeDelete(DeleteData data, Transaction tx) {
		QueryVerifier.verifyDeleteData(data, tx);
		Plan p = new TablePlan(data.tableName(), tx);
		p = new SelectPlan(p, data.pred());
		UpdateScan us = (UpdateScan) p.open();
		us.beforeFirst();
		int count = 0;
		while (us.next()) {
			us.delete();
			count++;
		}
		us.close();
		VanillaDB.mdMgr().countRecordUpdates(data.tableName(), count);
		return count;
	}

	@Override
	public int executeModify(ModifyData data, Transaction tx) {
		QueryVerifier.verifyModifyData(data, tx);
		Plan p = new TablePlan(data.tableName(), tx);
		p = new SelectPlan(p, data.pred());
		UpdateScan us = (UpdateScan) p.open();
		us.beforeFirst();
		int count = 0;
		while (us.next()) {
			Collection<String> targetflds = data.targetFields();
			for (String fld : targetflds)
				us.setVal(fld, data.newValue(fld).evaluate(us));
			count++;
		}
		us.close();
		VanillaDB.mdMgr().countRecordUpdates(data.tableName(), count);
		return count;
	}

	@Override
	public int executeInsert(InsertData data, Transaction tx) {
		QueryVerifier.verifyInsertData(data, tx);
		Plan p = new TablePlan(data.tableName(), tx);
		UpdateScan us = (UpdateScan) p.open();
		us.insert();
		Iterator<Constant> iter = data.vals().iterator();
		for (String fldname : data.fields())
			us.setVal(fldname, iter.next());

		us.close();
		VanillaDB.mdMgr().countRecordUpdates(data.tableName(), 1);
		return 1;
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
