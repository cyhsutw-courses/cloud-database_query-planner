package org.vanilladb.core.query.algebra;

import java.util.*;

import org.vanilladb.core.sql.Constant;


/**
 * The scan class corresponding to the <em>project</em> relational algebra
 * operator. All methods except hasField delegate their work to the underlying
 * scan.
 */
public class ProjectScan implements Scan {
	private Scan s;
	private Collection<String> fieldList;

	/**
	 * Creates a project scan having the specified underlying scan and field
	 * list.
	 * 
	 * @param s
	 *            the underlying scan
	 * @param fieldList
	 *            the list of field names
	 */
	public ProjectScan(Scan s, Collection<String> fieldList) {
		this.s = s;
		this.fieldList = fieldList;
	}

	@Override
	public void beforeFirst() {
		s.beforeFirst();
	}

	@Override
	public boolean next() {
		return s.next();
	}

	@Override
	public void close() {
		s.close();
	}

	@Override
	public Constant getVal(String fldName) {
		if (hasField(fldName))
			return s.getVal(fldName);
		else
			throw new RuntimeException("field " + fldName + " not found.");
	}

	/**
	 * Returns true if the specified field is in the projection list.
	 * 
	 * @see Scan#hasField(java.lang.String)
	 */
	@Override
	public boolean hasField(String fldName) {
		return fieldList.contains(fldName);
	}
}
