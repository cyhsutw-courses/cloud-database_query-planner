package org.vanilladb.core.storage.metadata;

import java.util.Set;
import java.util.TreeSet;

import org.vanilladb.core.sql.Constant;


public class FieldStatInfo {
	/**
	 * The maximum number of sample values kept in memory to extrapolate
	 * distinct values.
	 */
	private static final int MAX_SAMPLE_VALUES;

	private Set<Constant> values = new TreeSet<Constant>();
	private int sampleRecs;
	private Constant maxValue, minValue;

	static {
		String prop = System.getProperty(FieldStatInfo.class.getName()
				+ ".MAX_SAMPLE_VALUES");
		MAX_SAMPLE_VALUES = (prop == null ? 1000 : Integer
				.parseInt(prop.trim()));
	}

	public void addValue(Constant c) {
		if (values.size() < MAX_SAMPLE_VALUES) {
			values.add(c);
			sampleRecs++;
		}
		if (minValue == null || c.compareTo(minValue) < 0)
			minValue = c;
		else if (maxValue == null || c.compareTo(maxValue) > 0)
			maxValue = c;
	}

	public int distinctValues(int numRecs) {
		return numRecs * values.size() / sampleRecs;
	}

	public Constant maxValue() {
		return maxValue;
	}

	public Constant minValue() {
		return minValue;
	}
}