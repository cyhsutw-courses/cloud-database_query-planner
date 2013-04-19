package org.vanilladb.core.sql;

/**
 * A record.
 */
public interface Record {
	/**
	 * Returns the {@link Constant value} of the specified field.
	 * 
	 * @param fldName
	 *            the name of the field
	 * @return the value of that field
	 */
	Constant getVal(String fldName);
}
