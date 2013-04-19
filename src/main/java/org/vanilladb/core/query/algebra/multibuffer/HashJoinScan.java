package org.vanilladb.core.query.algebra.multibuffer;

import static org.vanilladb.core.sql.predicate.Term.OP_EQ;

import java.util.List;

import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.SelectScan;
import org.vanilladb.core.query.algebra.materialize.TempTable;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.predicate.Expression;
import org.vanilladb.core.sql.predicate.FieldNameExpression;
import org.vanilladb.core.sql.predicate.Predicate;
import org.vanilladb.core.sql.predicate.Term;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.tx.Transaction;


public class HashJoinScan implements Scan {
	private List<TempTable> tables1, tables2;
	private Transaction tx;
	private int currentIndex;
	private Scan current;
	private Predicate pred;

	public HashJoinScan(List<TempTable> tables1, List<TempTable> tables2,
			String fldname1, String fldname2, Transaction tx) {
		this.tables1 = tables1;
		this.tables2 = tables2;
		this.tx = tx;
		Expression exp1 = new FieldNameExpression(fldname1);
		Expression exp2 = new FieldNameExpression(fldname2);
		Term t = new Term(exp1, OP_EQ, exp2);
		pred = new Predicate(t);
		beforeFirst();
	}

	@Override
	public void beforeFirst() {
		openscan(0);
	}

	@Override
	public boolean next() {
		while (true) {
			if (current.next())
				return true;
			currentIndex++;
			if (currentIndex >= tables1.size())
				return false;
			openscan(currentIndex);
		}
	}

	private void openscan(int n) {
		close();
		currentIndex = n;
		Scan s1 = tables1.get(n).open();
		TableInfo ti2 = tables2.get(n).getTableInfo();
		Scan s3 = new MultiBufferProductScan(s1, ti2, tx);
		current = new SelectScan(s3, pred);
	}

	@Override
	public void close() {
		if (current != null)
			current.close();
	}

	@Override
	public Constant getVal(String fldname) {
		return current.getVal(fldname);
	}

	@Override
	public boolean hasField(String fldname) {
		return current.hasField(fldname);
	}
}
