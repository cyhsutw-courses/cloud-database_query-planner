package org.vanilladb.core.sql.aggfn;

import static org.vanilladb.core.sql.Type.INTEGER;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Record;
import org.vanilladb.core.sql.Type;

/**
 * The <em>count</em> aggregation function.
 */
public class CountFn extends AggregationFn {
	private String fldName;
	private int count;

	public CountFn(String fldName) {
		this.fldName = fldName;
	}

	@Override
	public void processFirst(Record rec) {
		count = 1;
	}

	@Override
	public void processNext(Record rec) {
		count++;
	}

	@Override
	public String argumentFieldName() {
		return fldName;
	}

	@Override
	public String fieldName() {
		return "countof" + fldName;
	}

	@Override
	public Constant value() {
		return new IntegerConstant(count);
	}

	@Override
	public Type fieldType() {
		return INTEGER;
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

		if (!(other instanceof CountFn))
			return false;

		CountFn otherCountFn = (CountFn) other;
		if (!fldName.equals(otherCountFn.fldName))
			return false;

		return true;
	}
}
