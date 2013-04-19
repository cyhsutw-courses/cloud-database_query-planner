package org.vanilladb.core.storage.tx;

public interface TransactionStartListener {
	/**
	 * Provides a hook for some object (e.g., a BufferMgr instance) to prepare
	 * and register the lifecycle listeners (e.g., the BufferMgr instance
	 * itself) to this stating tx
	 * 
	 * @param tx
	 */
	void onTxStart(Transaction tx);

}
