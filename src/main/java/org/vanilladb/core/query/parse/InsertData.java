package org.vanilladb.core.query.parse;

import java.util.*;

import org.vanilladb.core.sql.Constant;


/**
 * Data for the SQL <em>insert</em> statement.
 */
public class InsertData {
	private String tblName;
	private List<String> fields;
	private List<Constant> vals;

	/**
	 * Saves the table name and the field and value lists.
	 */
	public InsertData(String tblName, List<String> fields, List<Constant> vals) {
		this.tblName = tblName;
		this.fields = fields;
		this.vals = vals;
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
	 * Returns a list of fields for which values will be specified in the new
	 * record.
	 * 
	 * @return a list of field names
	 */
	public List<String> fields() {
		return fields;
	}

	/**
	 * Returns a list of values for the specified fields. There is a one-one
	 * correspondence between this list of values and the list of fields.
	 * 
	 * @return a list of Constant values.
	 */
	public List<Constant> vals() {
		return vals;
	}
}
