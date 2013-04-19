package org.vanilladb.core.query.algebra.materialize;

import static org.vanilladb.core.sql.RecordComparator.DIR_ASC;

import java.util.ArrayList;
import java.util.List;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.UpdateScan;
import org.vanilladb.core.sql.RecordComparator;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.tx.Transaction;


/**
 * The Plan class for the <em>sort</em> operator.
 */
public class SortPlan implements Plan {
	private Plan p;
	private RecordComparator comp;
	private Transaction tx;
	private Schema schema;

	/**
	 * Creates a sort plan for the specified query.
	 * 
	 * @param p
	 *            the plan for the underlying query
	 * @param sortFields
	 *            the fields to sort by
	 * @param sortDir
	 *            the sort direction
	 * @param tx
	 *            the calling transaction
	 */
	public SortPlan(Plan p, List<String> sortFields, List<Integer> sortDirs,
			Transaction tx) {
		this.p = p;
		comp = new RecordComparator(sortFields, sortDirs);
		this.tx = tx;
		schema = p.schema();
	}

	public SortPlan(Plan p, List<String> sortFields, Transaction tx) {
		this.p = p;
		List<Integer> sortDirs = new ArrayList<Integer>(sortFields.size());
		for (int i = 0; i < sortFields.size(); i++)
			sortDirs.add(DIR_ASC);
		comp = new RecordComparator(sortFields, sortDirs);
		this.tx = tx;
		schema = p.schema();
	}

	/**
	 * This method is where most of the action is. Up to 2 sorted temporary
	 * tables are created, and are passed into SortScan for final merging.
	 * 
	 * @see Plan#open()
	 */
	@Override
	public Scan open() {
		Scan src = p.open();
		List<TempTable> runs = splitIntoRuns(src);
		/*
		 * If the input source scan has no record, the temp table list will
		 * result in size 0. Need to check the size of "runs" here.
		 */
		if (runs.size() == 0)
			return src;
		src.close();
		while (runs.size() > 2)
			runs = doAMergeIteration(runs);
		return new SortScan(runs, comp);
	}

	/**
	 * Returns the number of blocks in the sorted table, which is the same as it
	 * would be in a materialized table. It does <em>not</em> include the
	 * one-time cost of materializing and sorting the records.
	 * 
	 * @see Plan#blocksAccessed()
	 */
	@Override
	public long blocksAccessed() {
		// does not include the one-time cost of sorting
		Plan mp = new MaterializePlan(p, tx); // not opened; just for analysis
		return mp.blocksAccessed();
	}

	/**
	 * Returns the schema of the sorted table, which is the same as in the
	 * underlying query.
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
		return p.histogram();
	}

	@Override
	public long recordsOutput() {
		return p.recordsOutput();
	}

	private List<TempTable> splitIntoRuns(Scan src) {
		List<TempTable> temps = new ArrayList<TempTable>();
		src.beforeFirst();
		if (!src.next())
			return temps;
		TempTable currenttemp = new TempTable(schema, tx);
		temps.add(currenttemp);
		UpdateScan currentscan = currenttemp.open();
		while (copy(src, currentscan)) {
			if (comp.compare(src, currentscan) < 0) {
				// start a new run
				currentscan.close();
				currenttemp = new TempTable(schema, tx);
				temps.add(currenttemp);
				currentscan = (UpdateScan) currenttemp.open();
			}
		}
		currentscan.close();
		return temps;
	}

	private List<TempTable> doAMergeIteration(List<TempTable> runs) {
		List<TempTable> result = new ArrayList<TempTable>();
		while (runs.size() > 1) {
			TempTable p1 = runs.remove(0);
			TempTable p2 = runs.remove(0);
			result.add(mergeTwoRuns(p1, p2));
		}
		if (runs.size() == 1)
			result.add(runs.get(0));
		return result;
	}

	private TempTable mergeTwoRuns(TempTable p1, TempTable p2) {
		Scan src1 = p1.open();
		Scan src2 = p2.open();
		TempTable result = new TempTable(schema, tx);
		UpdateScan dest = result.open();
		src1.beforeFirst();
		src2.beforeFirst();
		boolean hasmore1 = src1.next();
		boolean hasmore2 = src2.next();
		while (hasmore1 && hasmore2) {
			if (comp.compare(src1, src2) < 0)
				hasmore1 = copy(src1, dest);
			else
				hasmore2 = copy(src2, dest);
		}
		if (hasmore1)
			while (hasmore1)
				hasmore1 = copy(src1, dest);
		else
			while (hasmore2)
				hasmore2 = copy(src2, dest);
		src1.close();
		src2.close();
		dest.close();
		return result;
	}

	private boolean copy(Scan src, UpdateScan dest) {
		dest.insert();
		for (String fldname : schema.fields())
			dest.setVal(fldname, src.getVal(fldname));
		return src.next();
	}
	
	@Override
	public ArrayList<Plan> getUnderlyingPlan(){
		ArrayList<Plan> arr=new ArrayList<Plan>();
		arr.add(p);
		return arr;
	}
}
