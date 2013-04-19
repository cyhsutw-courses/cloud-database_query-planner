package org.vanilladb.core.sql.aggfn;

import static org.vanilladb.core.sql.Type.DOUBLE;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.Record;
import org.vanilladb.core.sql.Type;

/**
 * The <em>avg</em> aggregation function. Supports only a numeric field.
 */
public class AvgFn extends AggregationFn {
	private String fldName;
	private Constant sum;
	private int count;

	public AvgFn(String fldName) {
		this.fldName = fldName;
	}

	@Override
	public void processFirst(Record rec) {
		count = 1;
		this.sum = rec.getVal(fldName).castTo(DOUBLE);
	}

	@Override
	public void processNext(Record rec) {
		count++;
		sum = sum.add(rec.getVal(fldName));
	}

	@Override
	public String argumentFieldName() {
		return fldName;
	}

	@Override
	public String fieldName() {
		return "avgof" + fldName;
	}

	@Override
	public Constant value() {
		return sum.div(new DoubleConstant(this.count));
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

		if (!(other instanceof AvgFn))
			return false;

		AvgFn otherAvgFn = (AvgFn) other;
		if (!fldName.equals(otherAvgFn.fldName))
			return false;

		return true;
	}
}
