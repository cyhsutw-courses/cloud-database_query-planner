package org.vanilladb.core.query.algebra.materialize;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.vanilladb.core.query.algebra.AbstractJoinPlan;
import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.tx.Transaction;


/**
 * The Plan class for the <em>mergejoin</em> operator.
 */
public class MergeJoinPlan extends AbstractJoinPlan {
	private Plan sp1, sp2;
	private String fldName1, fldName2;
	private Schema schema = new Schema();
	private Histogram hist;

	/**
	 * Creates a mergejoin plan for the two specified queries. The RHS must be
	 * materialized after it is sorted, in order to deal with possible
	 * duplicates.
	 * 
	 * @param p1
	 *            the LHS query plan
	 * @param p2
	 *            the RHS query plan
	 * @param fldName1
	 *            the LHS join field
	 * @param fldName2
	 *            the RHS join field
	 * @param tx
	 *            the calling transaction
	 */
	public MergeJoinPlan(Plan p1, Plan p2, String fldName1, String fldName2,
			Transaction tx) {
		this.fldName1 = fldName1;
		List<String> sortlist1 = Arrays.asList(fldName1);
		this.sp1 = new SortPlan(p1, sortlist1, tx);
		this.fldName2 = fldName2;
		List<String> sortlist2 = Arrays.asList(fldName2);
		this.sp2 = new SortPlan(p2, sortlist2, tx);

		schema.addAll(p1.schema());
		schema.addAll(p2.schema());

		hist = joinHistogram(p1.histogram(), p2.histogram(), fldName1, fldName2);
	}

	/**
	 * The method first sorts its two underlying scans on their join field. It
	 * then returns a mergejoin scan of the two sorted table scans.
	 * 
	 * @see Plan#open()
	 */
	@Override
	public Scan open() {
		SortScan ss1 = (SortScan) sp1.open();
		SortScan ss2 = (SortScan) sp2.open();
		return new MergeJoinScan(ss1, ss2, fldName1, fldName2);
	}

	/**
	 * Returns the number of block acceses required to mergejoin the sorted
	 * tables. Since a mergejoin can be preformed with a single pass through
	 * each table, the method returns the sum of the block accesses of the
	 * materialized sorted tables. It does <em>not</em> include the one-time
	 * cost of materializing and sorting the records.
	 * 
	 * @see Plan#blocksAccessed()
	 */
	@Override
	public long blocksAccessed() {
		return sp1.blocksAccessed() + sp2.blocksAccessed();
	}

	/**
	 * Returns the schema of the join, which is the union of the schemas of the
	 * underlying queries.
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
		return (int) hist.recordsOutput();
	}
	
	@Override
	public ArrayList<Plan> getUnderlyingPlan(){
		ArrayList<Plan> arr=new ArrayList<Plan>();
		arr.add(sp1);
		arr.add(sp2);
		return arr;
	}
}
