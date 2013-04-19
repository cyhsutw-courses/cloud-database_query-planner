package org.vanilladb.core.sql;

import org.vanilladb.core.util.ByteHelper;

/**
 * The class that wraps Java doubles as database constants.
 */
public class DoubleConstant extends Constant {
	private Double val;

	/**
	 * Create a constant by wrapping the specified long.
	 * 
	 * @param n
	 *            the long value
	 */
	public DoubleConstant(double n) {
		val = n;
	}

	public DoubleConstant(byte[] v) {
		val = ByteHelper.toDouble(v);
	}

	/**
	 * Unwraps the Double and returns it.
	 * 
	 * @see Constant#asJavaVal()
	 */
	@Override
	public Object asJavaVal() {
		return val;
	}

	@Override
	public Type getType() {
		return Type.DOUBLE;
	}

	@Override
	public int size() {
		return ByteHelper.DOUBLE_SIZE;
	}

	@Override
	public byte[] asBytes() {
		return ByteHelper.toBytes(val);
	}

	@Override
	public Constant castTo(Type type) {
		if (getType().equals(type))
			return this;
		switch (type.getSqlType()) {
		case java.sql.Types.INTEGER:
			return new IntegerConstant(val.intValue());
		case java.sql.Types.BIGINT:
			return new BigIntConstant(val.longValue());
		case java.sql.Types.VARCHAR:
			return new VarcharConstant(val.toString(), type);
		}
		throw new IllegalArgumentException("Unspported constant type");
	}

	/**
	 * Indicates whether some other object is {@link Constant} object and its
	 * value equal to this one.
	 */
	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null)
			return false;
		return compareTo((Constant) obj) == 0;
	}

	@Override
	public int compareTo(Constant c) {
		if (c instanceof VarcharConstant)
			throw new IllegalArgumentException();
		else if (c instanceof IntegerConstant)
			return val.compareTo(((Integer) c.asJavaVal()).doubleValue());
		else if (c instanceof BigIntConstant)
			return val.compareTo(((Long) c.asJavaVal()).doubleValue());
		else if (c instanceof DoubleConstant) {
			return val.compareTo((Double) c.asJavaVal());
		} else
			throw new IllegalArgumentException();
	}

	@Override
	public int hashCode() {
		return val.hashCode();
	}

	@Override
	public String toString() {
		return val.toString();
	}

	@Override
	public Constant add(Constant c) {
		if (c instanceof VarcharConstant)
			throw new IllegalArgumentException();
		else if (c instanceof IntegerConstant)
			return new DoubleConstant(val + (Integer) c.asJavaVal());
		else if (c instanceof BigIntConstant) {
			return new DoubleConstant(val + (Long) c.asJavaVal());
		} else if (c instanceof DoubleConstant) {
			return new DoubleConstant(val + (Double) c.asJavaVal());
		} else
			throw new IllegalArgumentException();
	}

	@Override
	public Constant sub(Constant c) {
		if (c instanceof VarcharConstant)
			throw new IllegalArgumentException();
		else if (c instanceof IntegerConstant)
			return new DoubleConstant(val - (Integer) c.asJavaVal());
		else if (c instanceof BigIntConstant) {
			return new DoubleConstant(val - (Long) c.asJavaVal());
		} else if (c instanceof DoubleConstant) {
			return new DoubleConstant(val - (Double) c.asJavaVal());
		} else
			throw new IllegalArgumentException();
	}

	@Override
	public Constant div(Constant c) {
		if (c instanceof VarcharConstant)
			throw new IllegalArgumentException();
		else if (c instanceof IntegerConstant)
			return new DoubleConstant(val / (Integer) c.asJavaVal());
		else if (c instanceof BigIntConstant) {
			return new DoubleConstant(val / (Long) c.asJavaVal());
		} else if (c instanceof DoubleConstant) {
			return new DoubleConstant(val / (Double) c.asJavaVal());
		} else
			throw new IllegalArgumentException();
	}

	@Override
	public Constant mul(Constant c) {
		if (c instanceof VarcharConstant)
			throw new IllegalArgumentException();
		else if (c instanceof IntegerConstant)
			return new DoubleConstant(val * (Integer) c.asJavaVal());
		else if (c instanceof BigIntConstant) {
			return new DoubleConstant(val * (Long) c.asJavaVal());
		} else if (c instanceof DoubleConstant) {
			return new DoubleConstant(val * (Double) c.asJavaVal());
		} else
			throw new IllegalArgumentException();
	}
}
