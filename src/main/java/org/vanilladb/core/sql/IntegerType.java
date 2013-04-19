package org.vanilladb.core.sql;

import org.vanilladb.core.util.ByteHelper;

public class IntegerType extends Type {
	IntegerType() {
	}

	@Override
	public int getSqlType() {
		return java.sql.Types.INTEGER;
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
		return ByteHelper.INT_SIZE;
	}

	@Override
	public Constant maxValue() {
		return new IntegerConstant(Integer.MAX_VALUE);
	}

	@Override
	public Constant minValue() {
		return new IntegerConstant(Integer.MIN_VALUE);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null || !(obj instanceof IntegerType))
			return false;
		IntegerType t = (IntegerType) obj;
		return getSqlType() == t.getSqlType()
				&& getArgument() == t.getArgument();
	}

	@Override
	public String toString() {
		return "INTEGER";
	}

	@Override
	public int hashCode() {
		return toString().hashCode();
	}

}
