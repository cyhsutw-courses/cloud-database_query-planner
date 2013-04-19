package org.vanilladb.core.sql.aggfn;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Record;
import org.vanilladb.core.sql.Type;

/**
 * The interface implemented by aggregation functions.
 * <p>
 * Note that the aggregation function should implement {@link Object#hashCode()}
 * and {@link Object#equals(Object)} which are used to verify the equality of
 * aggregation functions.
 * </p>
 */
public abstract class AggregationFn {
	/**
	 * Processes the specified record by regarding it as the first record in a
	 * group.
	 * 
	 * @param rec
	 *            a record to aggregate over.
	 */
	public abstract void processFirst(Record rec);

	/**
	 * Processes the specified record by regarding it as a following up record
	 * in a group.
	 * 
	 * @param rec
	 *            a rec to aggregate over.
	 */
	public abstract void processNext(Record rec);

	/**
	 * Returns the name of the argument field.
	 * 
	 * @return the name of the argument field
	 */
	public abstract String argumentFieldName();

	/**
	 * Returns the name of the new aggregation field.
	 * 
	 * @return the name of the new aggregation field
	 */
	public abstract String fieldName();

	/**
	 * Returns the computed aggregation value given the records processed
	 * previously.
	 * 
	 * @return the computed aggregation value
	 */
	public abstract Constant value();

	/**
	 * Returns the type of aggregation value.
	 * 
	 * @return the type of aggregation value
	 */
	public abstract Type fieldType();

	/**
	 * Returns true if the type of aggregation value is depend on the argument
	 * field.
	 * 
	 * @return true if the type of aggregation value is depend on the argument
	 *         field
	 */
	public abstract boolean isArgumentTypeDependent();

	/**
	 * Returns a hash code value for the object.
	 */
	@Override
	public abstract int hashCode();

	/**
	 * Returns a hash code value for the object.
	 */
	@Override
	public abstract boolean equals(Object other);
}
