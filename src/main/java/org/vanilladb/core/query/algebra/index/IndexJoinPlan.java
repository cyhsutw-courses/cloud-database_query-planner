package org.vanilladb.core.query.algebra.index;

import java.util.ArrayList;

import org.vanilladb.core.query.algebra.AbstractJoinPlan;
import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.algebra.TableScan;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.tx.Transaction;


/**
 * The {@link Plan} class corresponding to the <em>indexjoin</em> relational
 * algebra operator.
 */
public class IndexJoinPlan extends AbstractJoinPlan {
	private Plan p1;
	private TablePlan tp2;
	private IndexInfo ii;
	private String joinField;
	private Schema schema = new Schema();
	private Transaction tx;
	private Histogram hist;

	/**
	 * Implements the join operator, using the specified LHS and RHS plans.
	 * 
	 * @param p1
	 *            the left-hand plan
	 * @param tp2
	 *            the right-hand table plan
	 * @param ii
	 *            information about the right-hand index
	 * @param joinField
	 *            the left-hand field used for joining
	 * @param tx
	 *            the calling transaction
	 */
	public IndexJoinPlan(Plan p1, TablePlan tp2, IndexInfo ii,
			String joinField, Transaction tx) {
		this.p1 = p1;
		this.tp2 = tp2;
		this.ii = ii;
		this.joinField = joinField;
		this.tx = tx;
		schema.addAll(p1.schema());
		schema.addAll(tp2.schema());
		hist = joinHistogram(p1.histogram(), tp2.histogram(), joinField,
				ii.fieldName());
	}

	/**
	 * Opens an indexjoin scan for this query
	 * 
	 * @see Plan#open()
	 */
	@Override
	public Scan open() {
		Scan s = p1.open();
		// throws an exception if p2 is not a tableplan
		TableScan ts = (TableScan) tp2.open();
		Index idx = ii.open(tx);
		return new IndexJoinScan(s, idx, joinField, ts);
	}

	/**
	 * Estimates the number of block accesses to compute the join. The formula
	 * is:
	 * 
	 * <pre>
	 * B(indexjoin(p1,p2,idx)) = B(p1) + R(p1)*B(idx)
	 *       + R(indexjoin(p1,p2,idx)
	 * </pre>
	 * 
	 * @see Plan#blocksAccessed()
	 */
	@Override
	public long blocksAccessed() {
		// block accesses to search for a join record in the index
		long searchCost = Index.searchCost(ii.indexType(),
				schema().type(ii.fieldName()), tp2.recordsOutput(), 1);
		return p1.blocksAccessed() + (p1.recordsOutput() * searchCost)
				+ recordsOutput();
	}

	/**
	 * Returns the schema of the index join.
	 * 
	 * @see Plan#schema()
	 */
	@Override
	public Schema schema() {
		return schema;
	}

	/**
	 * Returns the histogram that approximates the join distribution of the
	 * field values of query results.
	 * 
	 * @see Plan#histogram()
	 */
	@Override
	public Histogram histogram() {
		return hist;
	}

	@Override
	public long recordsOutput() {
		return (long) histogram().recordsOutput();
	}
	
	@Override
	public ArrayList<Plan> getUnderlyingPlan(){
		ArrayList<Plan> arr=new ArrayList<Plan>();
		arr.add(p1);
		arr.add(tp2);
		return arr;
	}
}
