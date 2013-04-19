package org.vanilladb.core.storage.index.btree;

import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.storage.index.btree.BTreeIndex.OP_DELETE;
import static org.vanilladb.core.storage.index.btree.BTreeIndex.OP_INSERT;
import static org.vanilladb.core.storage.index.btree.BTreeIndex.OP_SEARCH;

import java.util.ArrayList;

import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.concurrency.LockAbortException;


/**
 * A B-tree directory page that iterates over the B-tree directory blocks in a
 * file.
 * <p>
 * There is one flag in each B-tree directory block: the level (starting from 0
 * at the deepest) of that block in the directory.
 * </p>
 */
public class BTreeDir {
	/**
	 * A field name of the schema of B-tree directory records.
	 */
	static final String SCH_KEY = "key", SCH_CHILD = "child";

	static int NUM_FLAGS = 1;

	/**
	 * Returns the schema of the B-tree directory records.
	 * 
	 * @param fldType
	 *            the type of the indexed field
	 * 
	 * @return the schema of the index records
	 */
	static Schema schema(Type fldType) {
		Schema sch = new Schema();
		sch.addField(SCH_KEY, fldType);
		sch.addField(SCH_CHILD, BIGINT);
		return sch;
	}

	static long getLevelFlag(BTreePage p) {
		return p.getFlag(0);
	}

	static void setLevelFlag(BTreePage p, long val) {
		p.setFlag(0, val);
	}

	static Constant getKey(BTreePage p, int slot) {
		return p.getVal(slot, SCH_KEY);
	}

	static long getChildBlockNumber(BTreePage p, int slot) {
		return (Long) p.getVal(slot, SCH_CHILD).asJavaVal();
	}

	static void insert(BTreePage p, int slot, Constant val, long blkNum) {
		p.insert(slot);
		p.setVal(slot, SCH_KEY, val);
		p.setVal(slot, SCH_CHILD, new BigIntConstant(blkNum));
	}

	private TableInfo ti;
	private Transaction tx;
	private BTreePage contents;
	private String dataFileName;

	/**
	 * Creates an object to hold the contents of the specified B-tree block.
	 * 
	 * @param fileName
	 *            the data file name
	 * @param blk
	 *            a block ID refers to the specified B-tree block
	 * @param ti
	 *            the metadata of the B-tree directory file
	 * @param tx
	 *            the calling transaction
	 */
	BTreeDir(String dataFileName, BlockId blk, TableInfo ti, Transaction tx) {
		this.ti = ti;
		this.tx = tx;
		this.dataFileName = dataFileName;
		contents = new BTreePage(dataFileName, blk, NUM_FLAGS, ti, tx);
	}

	/**
	 * Closes the directory page.
	 */
	public void close() {
		contents.close();
	}

	/**
	 * Returns the block number of the B-tree leaf block that contains the
	 * specified search key.
	 * 
	 * @param searchKey
	 *            the search key
	 * @param action
	 *            the type of action
	 * @return the BlockId of the leaf block containing that search key
	 */
	public BlockId search(Constant searchKey, int action, TableInfo leafTi) {
		BlockId rootBlk = contents.currentBlk();
		if (action == OP_SEARCH) {
			long leafBlkNum = searchWithSLock(searchKey);
			BlockId leafBlk = new BlockId(leafTi.fileName(), leafBlkNum);
			sLockIndexBlock(leafBlk);
			tx.concurrencyMgr().releaseIndexBlocks(contents.currentBlk());
			return leafBlk;
		} else if (action == OP_DELETE) {
			long leafBlkNum = searchWithSLock(searchKey);
			BlockId leafBlk = new BlockId(leafTi.fileName(), leafBlkNum);
			xLockIndexBlock(leafBlk);
			tx.concurrencyMgr().releaseIndexBlocks(contents.currentBlk());
			return leafBlk;
		} else if (action == OP_INSERT) {
			long leafBlkNum = searchWithSLock(searchKey);
			BlockId leafBlk = new BlockId(leafTi.fileName(), leafBlkNum);
			xLockIndexBlock(leafBlk);
			BTreePage leaf = new BTreePage(dataFileName, leafBlk,
					BTreeLeaf.NUM_FLAGS, leafTi, tx);
			if (leaf.isGettingFull()
					|| searchKey.compareTo(BTreeLeaf.getKey(leaf, 0)) < 0) {
				tx.concurrencyMgr().releaseIndexBlocks(contents.currentBlk());
				tx.concurrencyMgr().releaseIndexBlocks(leafBlk);
				contents.close();
				contents = new BTreePage(dataFileName, rootBlk, NUM_FLAGS, ti,
						tx);
				leafBlk = xLockToLeaf(searchKey, leafTi);
			}
			leaf.close();
			return leafBlk;
		}
		throw new IllegalArgumentException();
	}

	private long searchWithSLock(Constant searchKey) {
		// search from root to level 0
		BlockId parentBlk = contents.currentBlk();
		sLockIndexBlock(parentBlk);
		long childBlkNum = findChildBlockNumber(searchKey);
		BlockId childBlk;
		while (getLevelFlag(contents) > 0) {
			// lock child
			childBlk = new BlockId(ti.fileName(), childBlkNum);
			sLockIndexBlock(childBlk);
			BTreePage child = new BTreePage(dataFileName, childBlk, NUM_FLAGS,
					ti, tx);
			// release parent block
			tx.concurrencyMgr().releaseIndexBlocks(parentBlk);
			contents.close();
			contents = child;
			childBlkNum = findChildBlockNumber(searchKey);
			parentBlk = contents.currentBlk();
		}
		// current blk not release slock yet
		return childBlkNum; // leaf block number
	}

	private BlockId xLockToLeaf(Constant searchKey, TableInfo leafTi) {
		// search from root to level 0
		xLockIndexBlock(contents.currentBlk());
		ArrayList<BlockId> parentBlks = new ArrayList<BlockId>();
		parentBlks.add(contents.currentBlk());
		long childBlkNum = findChildBlockNumber(searchKey);
		BlockId childBlk;
		while (getLevelFlag(contents) > 0) {
			// lock child
			childBlk = new BlockId(ti.fileName(), childBlkNum);
			xLockIndexBlock(childBlk);
			BTreePage child = new BTreePage(dataFileName, childBlk, NUM_FLAGS,
					ti, tx);
			// release parent blocks if child is safe
			if (!child.isGettingFull()) {
				tx.concurrencyMgr().releaseIndexBlocks(
						parentBlks.toArray(new BlockId[0]));
				parentBlks.clear();
			}
			contents.close();
			contents = child;
			childBlkNum = findChildBlockNumber(searchKey);
			parentBlks.add(contents.currentBlk());
		}
		BlockId leafBlk = new BlockId(leafTi.fileName(), childBlkNum);
		xLockIndexBlock(leafBlk);
		BTreePage leaf = new BTreePage(dataFileName, leafBlk,
				BTreeLeaf.NUM_FLAGS, leafTi, tx);
		if (!(leaf.isGettingFull() || searchKey.compareTo(BTreeLeaf.getKey(
				leaf, 0)) < 0)) {
			tx.concurrencyMgr().releaseIndexBlocks(
					parentBlks.toArray(new BlockId[0]));
			parentBlks.clear();
		}
		leaf.close();
		return leafBlk;
	}

	/**
	 * Creates a new root block for the B-tree. The new root will have two
	 * children: the old root, and the specified block. Since the root must
	 * always be in block 0 of the file, the contents of block 0 will get
	 * transferred to a new block (serving as the old root).
	 * 
	 * @param e
	 *            the directory entry to be added as a child of the new root
	 */
	public void makeNewRoot(DirEntry e) {
		if (contents.currentBlk().number() != 0) {
			contents.close();
			contents = new BTreePage(dataFileName,
					new BlockId(ti.fileName(), 0), NUM_FLAGS, ti, tx);
		}
		Constant firstval = getKey(contents, 0);
		long level = getLevelFlag(contents);
		// transfer all records to the new block
		long newBlkNum = contents.split(0, new long[] { level });
		DirEntry oldRootEntry = new DirEntry(firstval, newBlkNum);
		insertEntry(oldRootEntry);
		insertEntry(e);
		setLevelFlag(contents, level + 1);
	}

	/**
	 * Inserts a new directory entry into the B-tree directory block. If the
	 * block is at level 0, then the entry is inserted there. Otherwise, the
	 * entry is inserted into the appropriate child node, and the return value
	 * is examined. A non-null return value indicates that the child node
	 * splits, and so the returned entry is inserted into this block. If this
	 * block splits, then the method similarly returns the entry of the new
	 * block to its caller; otherwise, the method returns null.
	 * 
	 * @param e
	 *            the directory entry to be inserted
	 * @return the directory entry of the newly-split block, if one exists;
	 *         otherwise, null
	 */
	public DirEntry insert(DirEntry e) {
		if (getLevelFlag(contents) == 0)
			return insertEntry(e);
		long childBlkNum = findChildBlockNumber(e.key());
		BTreeDir child = new BTreeDir(dataFileName, new BlockId(ti.fileName(),
				childBlkNum), ti, tx);
		/*
		 * Recursive calls to the child's intert(). All the blocks in the
		 * calling stack will be pinned simultaneously.
		 */
		DirEntry myEntry = child.insert(e);
		child.close();
		return (myEntry != null) ? insertEntry(myEntry) : null;
	}

	private DirEntry insertEntry(DirEntry e) {
		int newslot = 1 + findSlotBefore(e.key());
		insert(contents, newslot, e.key(), e.blockNumber());
		if (!contents.isFull())
			return null;
		// split full page
		int splitPos = contents.getNumRecords() / 2;
		Constant splitVal = getKey(contents, splitPos);
		long newBlkNum = contents.split(splitPos,
				new long[] { getLevelFlag(contents) });
		return new DirEntry(splitVal, newBlkNum);
	}

	private long findChildBlockNumber(Constant searchKey) {
		int slot = findSlotBefore(searchKey);
		if (getKey(contents, slot + 1).equals(searchKey))
			slot++;
		return getChildBlockNumber(contents, slot);
	}

	/**
	 * Calculates the slot right before the one having the specified search key.
	 * 
	 * @param searchKey
	 *            the search key
	 * @return the position before where the search key goes
	 */
	private int findSlotBefore(Constant searchKey) {
		int slot = 0;
		while (slot < contents.getNumRecords()
				&& getKey(contents, slot).compareTo(searchKey) < 0)
			slot++;
		return slot - 1;
	}

	private void sLockIndexBlock(BlockId blk) {
		try {
			tx.concurrencyMgr().sLockIndexBlock(blk);
		} catch (LockAbortException e) {
			tx.rollback();
			throw e;
		}
	}

	private void xLockIndexBlock(BlockId blk) {
		try {
			tx.concurrencyMgr().xLockIndexBlock(blk);
		} catch (LockAbortException e) {
			tx.rollback();
			throw e;
		}
	}
}
