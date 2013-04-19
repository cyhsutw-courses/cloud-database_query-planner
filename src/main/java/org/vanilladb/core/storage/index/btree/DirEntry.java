package org.vanilladb.core.storage.index.btree;

import org.vanilladb.core.sql.Constant;

/**
 * A directory entry has two components: the key of the first record in that
 * block, and the number of the child block.
 */
public class DirEntry {
	private Constant key;
	private long blockNum;

	/**
	 * Creates a new entry for the specified key and block number.
	 * 
	 * @param key
	 *            the key
	 * @param blockNum
	 *            the block number
	 */
	public DirEntry(Constant key, long blockNum) {
		this.key = key;
		this.blockNum = blockNum;
	}

	/**
	 * Returns the key of the entry
	 * 
	 * @return the key of the entry
	 */
	public Constant key() {
		return key;
	}

	/**
	 * Returns the block number component of the entry
	 * 
	 * @return the block number component of the entry
	 */
	public long blockNumber() {
		return blockNum;
	}
}
