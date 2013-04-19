package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Record;

/**
 * The interface will be implemented by each query scan. There is a Scan class
 * for each relational algebra operator.
 * 
 * <p>
 * The {@link #beforeFirst()} method must be called before {@link #next()}.
 * </p>
 */
public interface Scan extends Record {

	/**
	 * Positions the scan before its first record.
	 */
	void beforeFirst();

	/**
	 * Moves the scan to the next record.
	 * 
	 * @return false if there is no next record
	 */
	boolean next();

	/**
	 * Closes the scan and its subscans, if any.
	 */
	void close();

	/**
	 * Returns true if the scan has the specified field.
	 * 
	 * @param fldname
	 *            the name of the field
	 * @return true if the scan has that field
	 */
	boolean hasField(String fldname);
}
