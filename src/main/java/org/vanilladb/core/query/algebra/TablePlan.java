package org.vanilladb.core.query.algebra;

import java.util.ArrayList;

import org.vanilladb.core.server.VanillaDB;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.TableNotFoundException;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.metadata.statistics.TableStatInfo;
import org.vanilladb.core.storage.tx.Transaction;


/**
 * The Plan class corresponding to a table.
 */
public class TablePlan implements Plan {
	private Transaction tx;
	private TableInfo ti;
	private TableStatInfo si;
	//TODO
	private String tableName;

	/**
	 * Creates a leaf node in the query tree corresponding to the specified
	 * table.
	 * 
	 * @param tblName
	 *            the name of the table
	 * @param tx
	 *            the calling transaction
	 */
	public TablePlan(String tblName, Transaction tx) {
		this.tx = tx;
		ti = VanillaDB.mdMgr().getTableInfo(tblName, tx);
		if (ti == null)
			throw new TableNotFoundException("table '" + tblName
					+ "' is not defined in catalog.");
		si = VanillaDB.mdMgr().getTableStatInfo(ti, tx);
		
		//TODO
		this.tableName=tblName;
	}

	//TODO
	/**
	 * Return TableName
	 * 
	 * @return table name
	 */
	@Override
	public String toString(){
		return tableName;
	}
	
	
	/**
	 * Creates a table scan for this query.
	 * 
	 * @see Plan#open()
	 */
	@Override
	public Scan open() {
		return new TableScan(ti, tx);
	}

	/**
	 * Estimates the number of block accesses for the table, which is obtainable
	 * from the statistics manager.
	 * 
	 * @see Plan#blocksAccessed()
	 */
	@Override
	public long blocksAccessed() {
		return si.blocksAccessed();
	}

	/**
	 * Determines the schema of the table, which is obtainable from the catalog
	 * manager.
	 * 
	 * @see Plan#schema()
	 */
	@Override
	public Schema schema() {
		return ti.schema();
	}

	/**
	 * Returns the histogram that approximates the join distribution of the
	 * field values of query results.
	 * 
	 * @see Plan#histogram()
	 */
	@Override
	public Histogram histogram() {
		return si.histogram();
	}

	@Override
	public long recordsOutput() {
		return (long) histogram().recordsOutput();
	}

	@Override
	public ArrayList<Plan> getUnderlyingPlan(){
		return null;
	}
}
