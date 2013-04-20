package org.vanilladb.core.query.algebra;

import java.util.ArrayList;
import java.util.Set;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.statistics.Histogram;


/**
 * The {@link Plan} class corresponding to the <em>project</em> relational
 * algebra operator.
 */
public class ProjectPlan implements Plan {
	/**
	 * Returns a histogram that approximates the join frequency distribution of
	 * the projected values from the specified histograms onto the specified
	 * fields.
	 * 
	 * @param hist
	 *            the input join distribution of field values
	 * @param fldNames
	 *            the names of fields to project to
	 * @return join distribution of projected values
	 */
	public static Histogram projectHistogram(Histogram hist,
			Set<String> fldNames) {
		Histogram pjtHist = new Histogram(fldNames);
		for (String fld : fldNames)
			pjtHist.setBuckets(fld, hist.buckets(fld));
		return pjtHist;
	}

	private Plan p;
	private Schema schema = new Schema();
	private Histogram hist;

	/**
	 * Creates a new project node in the query tree, having the specified
	 * subquery and field list.
	 * 
	 * @param p
	 *            the subquery
	 * @param fldNames
	 *            the list of fields
	 */
	public ProjectPlan(Plan p, Set<String> fldNames) {
		this.p = p;
		for (String fldname : fldNames)
			schema.add(fldname, p.schema());
		hist = projectHistogram(p.histogram(), fldNames);
	}

	/**
	 * Creates a project scan for this query.
	 * 
	 * @see Plan#open()
	 */
	@Override
	public Scan open() {
		Scan s = p.open();
		return new ProjectScan(s, schema.fields());
	}

	/**
	 * Estimates the number of block accesses in the projection, which is the
	 * same as in the underlying query.
	 * 
	 * @see Plan#blocksAccessed()
	 */
	@Override
	public long blocksAccessed() {
		return p.blocksAccessed();
	}

	/**
	 * Returns the schema of the projection, which is taken from the field list.
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
	public ArrayList<Plan> getUnderlyingPlans(){
		ArrayList<Plan> arr=new ArrayList<Plan>();
		arr.add(p);
		return arr;
	}
}
