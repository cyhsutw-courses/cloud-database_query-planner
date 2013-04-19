package org.vanilladb.core.storage.metadata;

/**
 * A runtime exception indicating that the table is not defined in the system
 * catalog.
 */
@SuppressWarnings("serial")
public class TableNotFoundException extends RuntimeException {
	public TableNotFoundException() {
	}

	public TableNotFoundException(String message) {
		super(message);
	}
}
