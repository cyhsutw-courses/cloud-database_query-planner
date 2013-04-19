package org.vanilladb.core.storage.tx;

public interface TransactionLifecycleListener {

	void onTxCommit(Transaction tx);

	void onTxRollback(Transaction tx);

	void onTxEndStatement(Transaction tx);

}
