package org.vanilladb.core.query.planner;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.parse.CreateIndexData;
import org.vanilladb.core.query.parse.CreateTableData;
import org.vanilladb.core.query.parse.CreateViewData;
import org.vanilladb.core.query.parse.DeleteData;
import org.vanilladb.core.query.parse.InsertData;
import org.vanilladb.core.query.parse.ModifyData;
import org.vanilladb.core.query.parse.Parser;
import org.vanilladb.core.query.parse.QueryData;
import org.vanilladb.core.storage.tx.Transaction;


/**
 * The object that executes SQL statements.
 * 
 * @author sciore
 */
public class Planner {
	private QueryPlanner qPlanner;
	private UpdatePlanner uPlanner;

	public Planner(QueryPlanner qPlanner, UpdatePlanner uPlanner) {
		this.qPlanner = qPlanner;
		this.uPlanner = uPlanner;
	}

	/**
	 * Creates a plan for an SQL select statement, using the supplied planner.
	 * 
	 * @param qry
	 *            the SQL query string
	 * @param tx
	 *            the transaction
	 * @return the scan corresponding to the query plan
	 */
	public Plan createQueryPlan(String qry, Transaction tx) {
		Parser parser = new Parser(qry);
		QueryData data = parser.query();
		return qPlanner.createPlan(data, tx);
	}

	/**
	 * Executes an SQL insert, delete, modify, or create statement. The method
	 * dispatches to the appropriate method of the supplied update planner,
	 * depending on what the parser returns.
	 * 
	 * @param cmd
	 *            the SQL update string
	 * @param tx
	 *            the transaction
	 * @return an integer denoting the number of affected records
	 */
	public int executeUpdate(String cmd, Transaction tx) {
		if (tx.isReadOnly())
			throw new UnsupportedOperationException();
		Parser parser = new Parser(cmd);
		Object obj = parser.updateCommand();
		if (obj instanceof InsertData)
			return uPlanner.executeInsert((InsertData) obj, tx);
		else if (obj instanceof DeleteData)
			return uPlanner.executeDelete((DeleteData) obj, tx);
		else if (obj instanceof ModifyData)
			return uPlanner.executeModify((ModifyData) obj, tx);
		else if (obj instanceof CreateTableData)
			return uPlanner.executeCreateTable((CreateTableData) obj, tx);
		else if (obj instanceof CreateViewData)
			return uPlanner.executeCreateView((CreateViewData) obj, tx);
		else if (obj instanceof CreateIndexData)
			return uPlanner.executeCreateIndex((CreateIndexData) obj, tx);
		else
			throw new UnsupportedOperationException();
	}
}
