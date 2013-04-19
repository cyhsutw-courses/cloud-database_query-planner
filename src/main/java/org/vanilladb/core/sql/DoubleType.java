package org.vanilladb.core.sql;

import org.vanilladb.core.util.ByteHelper;

public class DoubleType extends Type {
	DoubleType() {
	}

	@Override
	public int getSqlType() {
		return java.sql.Types.DOUBLE;
	}

	@Override
	public int getArgument() {
		return -1;
	}

	@Override
	public boolean isFixedSize() {
		return true;
	}

	@Override
	public boolean isNumeric() {
		return true;
	}

	@Override
	public int maxSize() {
		return ByteHelper.DOUBLE_SIZE;
	}

	@Override
	public Constant maxValue() {
		return new DoubleConstant(Double.MAX_VALUE);
	}

	@Override
	public Constant minValue() {
		return new DoubleConstant(Double.MIN_VALUE);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null || !(obj instanceof DoubleType))
			return false;
		DoubleType t = (DoubleType) obj;
		return getSqlType() == t.getSqlType()
				&& getArgument() == t.getArgument();
	}

	@Override
	public String toString() {
		return "DOUBLE";
	}

	@Override
	public int hashCode() {
		return toString().hashCode();
	}

}
