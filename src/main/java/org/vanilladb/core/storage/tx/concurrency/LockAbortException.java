package org.vanilladb.core.storage.tx.concurrency;

/**
 * A runtime exception indicating that the transaction needs to abort because a
 * lock could not be obtained.
 */
@SuppressWarnings("serial")
public class LockAbortException extends RuntimeException {
	public LockAbortException() {
	}
}
