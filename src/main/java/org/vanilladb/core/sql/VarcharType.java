package org.vanilladb.core.sql;

import java.nio.charset.Charset;

public class VarcharType extends Type {
	/**
	 * The name of charset used to encode/decode strings.
	 */
	public static final String CHAR_SET;
	/**
	 * Argument. -1 means undefined.
	 */
	private int argument = -1;

	static {
		String prop = System.getProperty(VarcharType.class.getName()
				+ ".CHAR_SET");
		CHAR_SET = (prop == null ? "UTF-8" : prop.trim());
	}

	VarcharType() {
	}

	VarcharType(int arg) {
		this.argument = arg;
	}

	@Override
	public int getSqlType() {
		return java.sql.Types.VARCHAR;
	}

	@Override
	public int getArgument() {
		return this.argument;
	}

	@Override
	public boolean isFixedSize() {
		return false;
	}

	@Override
	public boolean isNumeric() {
		return false;
	}

	/**
	 * Returns the maximum number of bytes required, by following the rule
	 * specified in {@link VarcharConstant#getBytes}, to encode a
	 * {@link Constant value} of this type.
	 */
	@Override
	public int maxSize() {
		// unlimited capacity if argument is not specified
		if (this.argument == -1)
			return Integer.MAX_VALUE;

		float bytesPerChar = Charset.forName(CHAR_SET).newEncoder()
				.maxBytesPerChar();
		return this.argument * (int) bytesPerChar;
	}

	@Override
	public Constant maxValue() {
		throw new UnsupportedOperationException();
	}

	/**
	 * Empty string is the minimal value by following the rules in
	 * {@link String#compareTo}.
	 */
	@Override
	public Constant minValue() {
		return new VarcharConstant("");
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null || !(obj instanceof VarcharType))
			return false;
		VarcharType t = (VarcharType) obj;
		return getSqlType() == t.getSqlType()
				&& getArgument() == t.getArgument();
	}

	@Override
	public String toString() {
		return "VARCHAR(" + this.argument + ")";
	}

	@Override
	public int hashCode() {
		return toString().hashCode();
	}

}
