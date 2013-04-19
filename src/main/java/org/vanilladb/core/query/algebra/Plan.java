package org.vanilladb.core.query.algebra;

import java.util.ArrayList;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.statistics.Histogram;


/**
 * The interface implemented by each query plan. There is a Plan class for each
 * relational algebra operator.
 */
public interface Plan {

	/**
	 * Opens a scan corresponding to this plan. The scan will be positioned
	 * before its first record.
	 * 
	 * @return a scan
	 */
	Scan open();

	/**
	 * Returns an estimate of the number of block accesses that will occur when
	 * the scan is read to completion.
	 * 
	 * @return the estimated number of block accesses
	 */
	long blocksAccessed();

	/**
	 * Returns the schema of the query.
	 * 
	 * @return the query's schema
	 */
	Schema schema();

	/**
	 * Returns the histogram that approximates the join distribution of the
	 * field values of query results.
	 * 
	 * @return the histogram
	 */
	Histogram histogram();

	/**
	 * Returns an estimate of the number of records in the query's output table.
	 * 
	 * @return the estimated number of output records
	 */
	long recordsOutput();
	
	/**
	 * Returns the underlying plan
	 * 
	 * @return the underlyingPlan
	 */
	ArrayList<Plan> getUnderlyingPlan();
	
}
