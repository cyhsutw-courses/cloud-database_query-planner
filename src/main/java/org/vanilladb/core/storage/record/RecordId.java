package org.vanilladb.core.storage.record;

import org.vanilladb.core.storage.file.BlockId;

/**
 * An identifier for a record within a file. A RecordId consists of the block
 * number in the file, and the ID of the record in that block.
 */
public class RecordId {
	private BlockId blk;
	private int id;

	/**
	 * Creates a record ID for the record having the specified ID in the
	 * specified block.
	 * 
	 * @param blkNum
	 *            the block number where the record lives
	 * @param id
	 *            the record's ID
	 */
	public RecordId(BlockId blk, int id) {
		this.blk = blk;
		this.id = id;
	}

	/**
	 * Returns the block associated with this record ID.
	 * 
	 * @return the block
	 */
	public BlockId block() {
		return blk;
	}

	/**
	 * Returns the ID associated with this record ID.
	 * 
	 * @return the ID
	 */
	public int id() {
		return id;
	}

	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null || !(obj instanceof RecordId))
			return false;
		RecordId r = (RecordId) obj;
		return blk.equals(r.blk) && id == r.id;
	}

	public String toString() {
		return "[file " + blk.fileName() + ", block " + blk.number()
				+ ", record " + id + "]";
	}

	public int hashCode() {
		return toString().hashCode();
	}
}
