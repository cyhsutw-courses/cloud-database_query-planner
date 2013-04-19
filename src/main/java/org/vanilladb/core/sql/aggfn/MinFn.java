package org.vanilladb.core.sql.aggfn;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Record;
import org.vanilladb.core.sql.Type;

/**
 * The <em>min</em> aggregation function.
 */
public class MinFn extends AggregationFn {
	private String fldName;
	private Constant val;

	public MinFn(String fldName) {
		this.fldName = fldName;
	}

	@Override
	public void processFirst(Record rec) {
		val = rec.getVal(fldName);
	}

	@Override
	public void processNext(Record rec) {
		Constant newval = rec.getVal(fldName);
		if (newval.compareTo(val) < 0)
			val = newval;
	}

	@Override
	public String argumentFieldName() {
		return fldName;
	}

	@Override
	public String fieldName() {
		return "minof" + fldName;
	}

	@Override
	public Constant value() {
		return val;
	}

	@Override
	public Type fieldType() {
		throw new IllegalStateException();
	}

	@Override
	public boolean isArgumentTypeDependent() {
		return true;
	}

	@Override
	public int hashCode() {
		return fieldName().hashCode();
	}

	@Override
	public boolean equals(Object other) {
		if (this == other)
			return true;

		if (!(other instanceof MinFn))
			return false;

		MinFn otherMinFn = (MinFn) other;
		if (!fldName.equals(otherMinFn.fldName))
			return false;

		return true;
	}
}
