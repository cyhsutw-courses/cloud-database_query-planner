package org.vanilladb.core.query.algebra.multibuffer;

import java.util.ArrayList;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.ProductPlan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.UpdateScan;
import org.vanilladb.core.query.algebra.materialize.MaterializePlan;
import org.vanilladb.core.query.algebra.materialize.TempTable;
import org.vanilladb.core.server.VanillaDB;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.tx.Transaction;


/**
 * The Plan class for the muti-buffer version of the <em>product</em> operator.
 */
public class MultiBufferProductPlan implements Plan {
	private Plan lhs, rhs;
	private Transaction tx;
	private Schema schema;
	private Histogram hist;

	/**
	 * Creates a product plan for the specified queries.
	 * 
	 * @param lhs
	 *            the plan for the LHS query
	 * @param rhs
	 *            the plan for the RHS query
	 * @param tx
	 *            the calling transaction
	 */
	public MultiBufferProductPlan(Plan lhs, Plan rhs, Transaction tx) {
		this.lhs = lhs;
		this.rhs = rhs;
		this.tx = tx;
		schema = new Schema();
		schema.addAll(lhs.schema());
		schema.addAll(rhs.schema());
		hist = ProductPlan.productHistogram(lhs.histogram(), rhs.histogram());
	}

	/**
	 * A scan for this query is created and returned, as follows. First, the
	 * method materializes its RHS query. It then determines the optimal chunk
	 * size, based on the size of the materialized file and the number of
	 * available buffers. It creates a chunk plan for each chunk, saving them in
	 * a list. Finally, it creates a multiscan for this list of plans, and
	 * returns that scan.
	 * 
	 * @see Plan#open()
	 */
	@Override
	public Scan open() {
		TempTable tt = copyRecordsFrom(rhs);
		TableInfo ti = tt.getTableInfo();
		Scan leftscan = lhs.open();
		return new MultiBufferProductScan(leftscan, ti, tx);
	}

	/**
	 * Returns an estimate of the number of block accesses required to execute
	 * the query. The formula is:
	 * 
	 * <pre>
	 * B(product(p1, p2)) = B(p2) + B(p1) * C(p2)
	 * </pre>
	 * 
	 * where C(p2) is the number of chunks of p2. The method uses the current
	 * number of available buffers to calculate C(p2), and so this value may
	 * differ when the query scan is opened.
	 * 
	 * @see Plan#blocksAccessed()
	 */
	@Override
	public long blocksAccessed() {
		// this guesses at the # of chunks
		int avail = VanillaDB.bufferMgr().available();
		long size = new MaterializePlan(rhs, tx).blocksAccessed();
		long numchunks = size / avail;
		return rhs.blocksAccessed() + (lhs.blocksAccessed() * numchunks);
	}

	/**
	 * Returns the schema of the product, which is the union of the schemas of
	 * the underlying queries.
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
		return (long) hist.recordsOutput();
	}

	@Override
	public ArrayList<Plan> getUnderlyingPlans(){
		ArrayList<Plan> arr=new ArrayList<Plan>();
		arr.add(lhs);
		arr.add(rhs);
		return arr;
	}
	
	private TempTable copyRecordsFrom(Plan p) {
		Scan src = p.open();
		Schema sch = p.schema();
		TempTable tt = new TempTable(sch, tx);
		UpdateScan dest = (UpdateScan) tt.open();
		src.beforeFirst();
		while (src.next()) {
			dest.insert();
			for (String fldname : sch.fields())
				dest.setVal(fldname, src.getVal(fldname));
		}
		src.close();
		dest.close();
		return tt;
	}
}