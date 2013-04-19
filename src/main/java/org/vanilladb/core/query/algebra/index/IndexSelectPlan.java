package org.vanilladb.core.query.algebra.index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.SelectPlan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.algebra.TableScan;
import org.vanilladb.core.sql.ConstantRange;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.tx.Transaction;


/**
 * The Plan class corresponding to the <em>indexselect</em> relational algebra
 * operator.
 */
public class IndexSelectPlan implements Plan {
	private TablePlan tp;
	private IndexInfo ii;
	private ConstantRange searchRange;
	private Transaction tx;
	private Histogram hist;

	/**
	 * Creates a new indexselect node in the query tree for the specified index
	 * and search range.
	 * 
	 * @param tp
	 *            the input table plan
	 * @param ii
	 *            information about the index
	 * @param searchRange
	 *            the range of search keys
	 * @param tx
	 *            the calling transaction
	 */
	public IndexSelectPlan(TablePlan tp, IndexInfo ii,
			ConstantRange searchRange, Transaction tx) {
		this.tp = tp;
		this.ii = ii;
		this.searchRange = searchRange;
		this.tx = tx;
		Map<String, ConstantRange> ranges = new HashMap<String, ConstantRange>();
		ranges.put(ii.fieldName(), searchRange);
		hist = SelectPlan.constantRangeHistogram(tp.histogram(), ranges);
	}

	/**
	 * Creates a new indexselect scan for this query
	 * 
	 * @see Plan#open()
	 */
	@Override
	public Scan open() {
		// throws an exception if p is not a tableplan.
		TableScan ts = (TableScan) tp.open();
		Index idx = ii.open(tx);
		return new IndexSelectScan(idx, searchRange, ts);
	}

	/**
	 * Estimates the number of block accesses to compute the index selection,
	 * which is the same as the index traversal cost plus the number of matching
	 * data records.
	 * 
	 * @see Plan#blocksAccessed()
	 */
	@Override
	public long blocksAccessed() {
		return Index.searchCost(ii.indexType(), schema().type(ii.fieldName()),
				tp.recordsOutput(), recordsOutput()) + recordsOutput();
	}

	/**
	 * Returns the schema of the data table.
	 * 
	 * @see Plan#schema()
	 */
	@Override
	public Schema schema() {
		return tp.schema();
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
		arr.add(tp);
		return arr;
	}
}
