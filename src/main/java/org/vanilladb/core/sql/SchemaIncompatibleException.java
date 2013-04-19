package org.vanilladb.core.sql;

/**
 * A runtime exception indicating that the string transaction wants to write
 * exceeds limit in schema definition.
 */
@SuppressWarnings("serial")
public class SchemaIncompatibleException extends RuntimeException {
	public SchemaIncompatibleException() {
	}

	public SchemaIncompatibleException(String message) {
		super(message);
	}
}
