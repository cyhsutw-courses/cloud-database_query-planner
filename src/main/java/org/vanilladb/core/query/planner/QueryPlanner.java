package org.vanilladb.core.query.planner;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.parse.QueryData;
import org.vanilladb.core.storage.tx.Transaction;


/**
 * The interface implemented by planners for the SQL select statement.
 */
public interface QueryPlanner {

	/**
	 * Creates a plan for the parsed query.
	 * 
	 * @param data
	 *            the parsed representation of the query
	 * @param tx
	 *            the calling transaction
	 * @return a plan for that query
	 */
	Plan createPlan(QueryData data, Transaction tx);
}
