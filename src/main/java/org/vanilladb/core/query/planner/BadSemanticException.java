package org.vanilladb.core.query.planner;

/**
 * A runtime exception indicating that the submitted query has incorrect
 * semantic. For example, the mentioned field or table name in the query is not
 * existed.
 */
@SuppressWarnings("serial")
public class BadSemanticException extends RuntimeException {
	public BadSemanticException() {
	}

	public BadSemanticException(String message) {
		super(message);
	}
}