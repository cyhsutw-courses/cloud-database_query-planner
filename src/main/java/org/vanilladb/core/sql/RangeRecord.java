package org.vanilladb.core.sql;

/**
 * A record whose each field value is a {@link ConstantRange constant range}.
 */
public interface RangeRecord {
	/**
	 * Returns the {@link ConstantRange value range} of the specified field.
	 * 
	 * @param fldName
	 *            the name of the field
	 * @return the value range of that field
	 */
	ConstantRange getVal(String fldName);
}
