package org.vanilladb.core.sql.aggfn;

import static org.vanilladb.core.sql.Type.DOUBLE;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Record;
import org.vanilladb.core.sql.Type;

/**
 * The <em>sum</em> aggregation function.
 */
public class SumFn extends AggregationFn {
	private String fldName;
	private Constant val;

	public SumFn(String fldName) {
		this.fldName = fldName;
	}

	@Override
	public void processFirst(Record rec) {
		Constant c = rec.getVal(fldName);
		this.val = c.castTo(DOUBLE);
	}

	@Override
	public void processNext(Record rec) {
		Constant newval = rec.getVal(fldName);
		val = val.add(newval);
	}

	@Override
	public String argumentFieldName() {
		return fldName;
	}

	@Override
	public String fieldName() {
		return "sumof" + fldName;
	}

	@Override
	public Constant value() {
		return val;
	}

	@Override
	public Type fieldType() {
		return DOUBLE;
	}

	@Override
	public boolean isArgumentTypeDependent() {
		return false;
	}

	@Override
	public int hashCode() {
		return fieldName().hashCode();
	}

	@Override
	public boolean equals(Object other) {
		if (this == other)
			return true;

		if (!(other instanceof SumFn))
			return false;

		SumFn otherSumFn = (SumFn) other;
		if (!fldName.equals(otherSumFn.fldName))
			return false;

		return true;
	}
}
