package org.vanilladb.core.query.parse;

/**
 * Data for the SQL <em>create view</em> statement.
 */
public class CreateViewData {
	private String viewName;
	private QueryData qryData;

	/**
	 * Saves the view name and its definition.
	 */
	public CreateViewData(String viewName, QueryData qryData) {
		this.viewName = viewName;
		this.qryData = qryData;
	}

	/**
	 * Returns the name of the new view.
	 * 
	 * @return the name of the new view
	 */
	public String viewName() {
		return viewName;
	}

	/**
	 * Returns the definition of the new view.
	 * 
	 * @return the definition of the new view
	 */
	public String viewDef() {
		return qryData.toString();
	}

	/**
	 * Returns the query data of the view definition.
	 * 
	 * @return the query data of the view definition
	 */
	public QueryData viewDefData() {
		return qryData;
	}
}
