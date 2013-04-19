package org.vanilladb.core.query.parse;

import org.vanilladb.core.sql.predicate.Predicate;

/**
 * Data for the SQL <em>delete</em> statement.
 */
public class DeleteData {
	private String tblName;
	private Predicate pred;

	/**
	 * Saves the table name and predicate.
	 */
	public DeleteData(String tblName, Predicate pred) {
		this.tblName = tblName;
		this.pred = pred;
	}

	/**
	 * Returns the name of the affected table.
	 * 
	 * @return the name of the affected table
	 */
	public String tableName() {
		return tblName;
	}

	/**
	 * Returns the predicate that describes which records should be deleted.
	 * 
	 * @return the deletion predicate
	 */
	public Predicate pred() {
		return pred;
	}
}
