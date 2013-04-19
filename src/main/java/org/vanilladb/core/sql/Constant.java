package org.vanilladb.core.sql;

import static java.sql.Types.BIGINT;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;

/**
 * An abstract class that denotes a value of a supported {@link Type type}.
 */
public abstract class Constant implements Comparable<Constant> {
	/**
	 * Constructs a new instance of the specified type with value converted from
	 * the input byte array.
	 * 
	 * @param type
	 *            the specified type
	 * @param val
	 *            the byte array contains the value
	 * @return a constant of specified type with value converted from the byte
	 *         array
	 */
	public static Constant newInstance(Type type, byte[] val) {
		switch (type.getSqlType()) {
		case (INTEGER):
			return new IntegerConstant(val);
		case (BIGINT):
			return new BigIntConstant(val);
		case (DOUBLE):
			return new DoubleConstant(val);
		case (VARCHAR):
			return new VarcharConstant(val, type);
		}
		throw new UnsupportedOperationException("Unspported SQL type: "
				+ type.getSqlType());
	}

	/**
	 * Constructs a new instance of the specified type with default value. For
	 * all numeric constants, the default value is 0; for string constants, the
	 * default value is an empty string.
	 * 
	 * @param type
	 *            the specified type
	 * @return the constant of specified type with default value
	 */
	public static Constant defaultInstance(Type type) {
		switch (type.getSqlType()) {
		case (INTEGER):
			return new IntegerConstant(0);
		case (BIGINT):
			return new BigIntConstant(0);
		case (DOUBLE):
			return new DoubleConstant(0);
		case (VARCHAR):
			return new VarcharConstant("");
		}
		throw new UnsupportedOperationException("Unspported SQL type: "
				+ type.getSqlType());
	}

	/**
	 * Returns the type corresponding to this constant.
	 * 
	 * @return the type of the constant
	 */
	public abstract Type getType();

	/**
	 * Returns the Java object corresponding to this constant.
	 * 
	 * @return the Java value of the constant
	 */
	public abstract Object asJavaVal();

	/**
	 * Returns the byte array corresponding to this constant value.
	 * 
	 * @return the byte array of the constant
	 */
	public abstract byte[] asBytes();

	/**
	 * Return the number of bytes required to encode this constant.
	 * 
	 * @return the size of the this constant
	 */
	public abstract int size();

	/**
	 * Casts this constant to the specified type. Does not support casting from
	 * a string constant to a numeric constant.
	 * 
	 * @return the constant casted to the specified type
	 */
	public abstract Constant castTo(Type type);

	/**
	 * Adds this constant a value specified by another constant. Does not
	 * support adding value to a string constant. Automatic type upgrade will be
	 * done during the arithmetic.
	 * 
	 * @param c
	 *            constant whose value will be added
	 * @return a constant with value equals to the sum of the value of this
	 *         constant and another constant
	 */
	public abstract Constant add(Constant c);

	/**
	 * Subtracts a value specified by another constant from this constant. Does
	 * not support adding value to a string constant. Automatic type upgrade
	 * will be done during the arithmetic.
	 * 
	 * @param c
	 *            constant whose value will be substracted from
	 * @return a constant with value equals to the value of this constant
	 *         subtracted from that of another constant
	 */
	public abstract Constant sub(Constant c);

	/**
	 * Multiplies this constant by a value specified by another constant. Does
	 * not support adding value to a string constant. Automatic type upgrade
	 * will be done during the arithmetic.
	 * 
	 * @param c
	 *            multiplier
	 * @return a constant with value equals to the value of this constant
	 *         multiplied by that of another constant
	 */
	public abstract Constant mul(Constant c);

	/**
	 * Divides this constant by a value specified by another constant. Does not
	 * support adding value to a string constant. Automatic type upgrade will be
	 * done during the arithmetic.
	 * 
	 * @param c
	 *            divider
	 * @return a constant with value equals to the value of this constant
	 *         divided by that of another constant
	 */
	public abstract Constant div(Constant c);
}