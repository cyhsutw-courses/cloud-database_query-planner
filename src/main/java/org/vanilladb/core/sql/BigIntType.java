package org.vanilladb.core.sql;

import org.vanilladb.core.util.ByteHelper;

public class BigIntType extends Type {
	BigIntType() {
	}

	@Override
	public int getSqlType() {
		return java.sql.Types.BIGINT;
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
		return ByteHelper.LONG_SIZE;
	}

	@Override
	public Constant maxValue() {
		return new BigIntConstant(Long.MAX_VALUE);
	}

	@Override
	public Constant minValue() {
		return new BigIntConstant(Long.MIN_VALUE);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null || !(obj instanceof BigIntType))
			return false;
		BigIntType t = (BigIntType) obj;
		return getSqlType() == t.getSqlType()
				&& getArgument() == t.getArgument();
	}

	@Override
	public String toString() {
		return "BIGINT";
	}

	@Override
	public int hashCode() {
		return toString().hashCode();
	}

}
