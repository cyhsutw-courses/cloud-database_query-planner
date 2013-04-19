package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Constant;

/**
 * The scan class corresponding to the <em>product</em> relational algebra
 * operator.
 */
public class ProductScan implements Scan {
	private Scan s1, s2;
	private boolean isLhsEmpty;

	/**
	 * Creates a product scan having the two underlying scans.
	 * 
	 * @param s1
	 *            the LHS scan
	 * @param s2
	 *            the RHS scan
	 */
	public ProductScan(Scan s1, Scan s2) {
		this.s1 = s1;
		this.s2 = s2;
		s1.beforeFirst();
		isLhsEmpty = !s1.next();
	}

	/**
	 * Positions the scan before its first record. In other words, the LHS scan
	 * is positioned at its first record, and the RHS scan is positioned before
	 * its first record.
	 * 
	 * @see Scan#beforeFirst()
	 */
	@Override
	public void beforeFirst() {
		s1.beforeFirst();
		isLhsEmpty = !s1.next();
		s2.beforeFirst();
	}

	/**
	 * Moves the scan to the next record. The method moves to the next RHS
	 * record, if possible. Otherwise, it moves to the next LHS record and the
	 * first RHS record. If there are no more LHS records, the method returns
	 * false.
	 * 
	 * @see Scan#next()
	 */
	@Override
	public boolean next() {
		if (isLhsEmpty)
			return false;
		if (s2.next())
			return true;
		else if (!(isLhsEmpty = !s1.next())) {
			s2.beforeFirst();
			return s2.next();
		} else {
			return false;
		}
	}

	/**
	 * Closes both underlying scans.
	 * 
	 * @see Scan#close()
	 */
	@Override
	public void close() {
		s1.close();
		s2.close();
	}

	/**
	 * Returns the value of the specified field. The value is obtained from
	 * whichever scan contains the field.
	 * 
	 * @see Scan#getVal(java.lang.String)
	 */
	@Override
	public Constant getVal(String fldName) {
		if (s1.hasField(fldName))
			return s1.getVal(fldName);
		else
			return s2.getVal(fldName);
	}

	/**
	 * Returns true if the specified field is in either of the underlying scans.
	 * 
	 * @see Scan#hasField(java.lang.String)
	 */
	@Override
	public boolean hasField(String fldName) {
		return s1.hasField(fldName) || s2.hasField(fldName);
	}
}
