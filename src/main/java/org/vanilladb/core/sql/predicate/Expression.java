package org.vanilladb.core.sql.predicate;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Record;
import org.vanilladb.core.sql.Schema;

/**
 * A SQL expression.
 */
public interface Expression {
	/**
	 * Returns true if the expression is a constant.
	 * 
	 * @return true if the expression is a constant
	 */
	boolean isConstant();

	/**
	 * Returns true if the expression is a field reference.
	 * 
	 * @return true if the expression denotes a field
	 */
	boolean isFieldName();

	/**
	 * Returns the constant corresponding to a constant expression. Throws an
	 * exception if this expression does not denote a constant.
	 * 
	 * @return the expression as a constant
	 */
	Constant asConstant();

	/**
	 * Returns the field name corresponding to a field name expression. Throws
	 * an exception if this expression does not denote a field.
	 * 
	 * @return the expression as a field name
	 */
	String asFieldName();

	/**
	 * Evaluates the expression with respect to the specified record.
	 * 
	 * @param rec
	 *            the record
	 * @return the value of the expression, as a constant
	 */
	Constant evaluate(Record rec);

	/**
	 * Determines if all of the fields mentioned in this expression are
	 * contained in the specified schema.
	 * 
	 * @param sch
	 *            the schema
	 * @return true if all fields in the expression are in the schema
	 */
	boolean isApplicableTo(Schema sch);
}
