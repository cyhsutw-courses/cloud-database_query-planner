package org.vanilladb.core.query.parse;

import org.vanilladb.core.sql.Schema;

/**
 * Data for the SQL <em>create table</em> statement.
 */
public class CreateTableData {
	private String tblName;
	private Schema schema;

	/**
	 * Saves the table name and schema.
	 */
	public CreateTableData(String tblName, Schema schema) {
		this.tblName = tblName;
		this.schema = schema;
	}

	/**
	 * Returns the name of the new table.
	 * 
	 * @return the name of the new table
	 */
	public String tableName() {
		return tblName;
	}

	/**
	 * Returns the schema of the new table.
	 * 
	 * @return the schema of the new table
	 */
	public Schema newSchema() {
		return schema;
	}
}
