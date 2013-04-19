package org.vanilladb.core.sql;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * The schema of a table, which contains the name and type of each field of the
 * table.
 */
public class Schema {
	private Map<String, Type> fields = new HashMap<String, Type>();

	/**
	 * Creates an empty schema.
	 */
	public Schema() {
	}

	/**
	 * Adds a field to this schema having a specified name and type.
	 * 
	 * @param fldName
	 *            the name of the field
	 * @param type
	 *            the type of the field, according to the constants in
	 *            {@link Type}
	 */
	public void addField(String fldName, Type type) {
		fields.put(fldName, type);
	}

	/**
	 * Adds a field in another schema having the specified name to this schema.
	 * 
	 * @param fldName
	 *            the name of the field
	 * @param sch
	 *            the other schema
	 */
	public void add(String fldName, Schema sch) {
		Type type = sch.type(fldName);
		addField(fldName, type);
	}

	/**
	 * Adds all of the fields in the specified schema to this schema.
	 * 
	 * @param sch
	 *            the other schema
	 */
	public void addAll(Schema sch) {
		fields.putAll(sch.fields);
	}

	/**
	 * Returns a sorted set containing the field names in this schema, sorted by
	 * their natural ordering.
	 * 
	 * @return the sorted set of the schema's field names
	 */
	public SortedSet<String> fields() {
		return new TreeSet<String>(fields.keySet());
	}

	/**
	 * Returns true if the specified field is in this schema.
	 * 
	 * @param fldName
	 *            the name of the field
	 * @return true if the field is in this schema
	 */
	public boolean hasField(String fldName) {
		return fields().contains(fldName);
	}

	/**
	 * Returns the type of the specified field.
	 * 
	 * @param fldName
	 *            the name of the field
	 * @return the type of the field
	 */
	public Type type(String fldName) {
		return fields.get(fldName);
	}
}
