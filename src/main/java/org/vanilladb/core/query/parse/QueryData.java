package org.vanilladb.core.query.parse;

import static org.vanilladb.core.sql.RecordComparator.DIR_DESC;

import java.util.List;
import java.util.Set;

import org.vanilladb.core.sql.RecordComparator;
import org.vanilladb.core.sql.aggfn.AggregationFn;
import org.vanilladb.core.sql.predicate.Predicate;


/**
 * Data for the SQL <em>select</em> statement.
 */
public class QueryData {
	private Set<String> projFields;
	private Set<String> tables;
	private Predicate pred;
	private Set<String> groupFields;
	private Set<AggregationFn> aggFn;
	private List<String> sortFields;
	private List<Integer> sortDirs;
	//TODO
	private boolean explain;

	/**
	 * Saves the field and table list and predicate.
	 */
	public QueryData(Set<String> projFields, Set<String> tables,
			Predicate pred, Set<String> groupFields, Set<AggregationFn> aggFn,
			List<String> sortFields, List<Integer> sortDirs, boolean explain) {
		this.projFields = projFields;
		this.tables = tables;
		this.pred = pred;
		this.groupFields = groupFields;
		this.aggFn = aggFn;
		this.sortFields = sortFields;
		this.sortDirs = sortDirs;
		//TODO
		this.explain=explain;
	}

	/**
	 * Returns the fields mentioned in the select clause.
	 * 
	 * @return a collection of field names
	 */
	public Set<String> projectFields() {
		return projFields;
	}

	/**
	 * Returns the tables mentioned in the from clause.
	 * 
	 * @return a collection of table names
	 */
	public Set<String> tables() {
		return tables;
	}

	/**
	 * Returns the predicate that describes which records should be in the
	 * output table.
	 * 
	 * @return the query predicate
	 */
	public Predicate pred() {
		return pred;
	}

	/**
	 * Returns the fields used to sort the query result.
	 * 
	 * @return a list of field names
	 */
	public List<String> sortFields() {
		return sortFields;
	}

	/**
	 * Returns a list of sort directions to the sorting fields. The values of
	 * sort directions are defined in {@link RecordComparator}.
	 * 
	 * @return a list of sort directions
	 */
	public List<Integer> sortDirections() {
		return sortDirs;
	}

	/**
	 * Returns the field names mentioned in the group by clause.
	 * 
	 * @return a collection of grouping field names
	 */
	public Set<String> groupFields() {
		return groupFields;
	}

	/**
	 * Returns the aggregation functions mentioned in the clause.
	 * 
	 * @return a collection of aggregation functions
	 */
	public Set<AggregationFn> aggregationFn() {
		return aggFn;
	}

	//TODO
	/**
	 * Returns if the query is a 'explain' query
	 * 
	 * @return boolean that indicate if the query is a 'explain query'
	 */
	public boolean explain(){
		return explain;
	}
	
	public String toString() {
		//TODO
		StringBuilder result;
		if(explain){
			result = new StringBuilder("explain select ");
		}else{
			result = new StringBuilder("select ");
		}
		
		for (String fldname : projFields)
			result.append(fldname + ", ");
		// remove the last comma
		result.delete(result.length() - 2, result.length());
		result.append(" from ");
		for (String tblname : tables)
			result.append(tblname + ", ");
		result.delete(result.length() - 2, result.length());

		String predString = pred.toString();
		if (!predString.equals(""))
			result.append(" where " + predString);

		if (groupFields != null) {
			result.append(" group by ");
			for (String gbf : groupFields)
				result.append(gbf + ", ");
			result.delete(result.length() - 2, result.length());
		}

		if (sortFields != null) {
			result.append(" sort by ");
			for (int i = 0; i < sortFields.size(); i++) {
				String sbf = sortFields.get(i);
				int sbd = sortDirs.get(i);
				result.append(sbf + (sbd == DIR_DESC ? " desc" : "") + ", ");
			}
			result.delete(result.length() - 2, result.length());
		}
		return result.toString();
	}
}