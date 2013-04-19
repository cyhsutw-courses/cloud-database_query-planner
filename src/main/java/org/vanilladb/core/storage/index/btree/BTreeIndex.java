package org.vanilladb.core.storage.index.btree;

import static org.vanilladb.core.storage.file.Page.BLOCK_SIZE;

import org.vanilladb.core.server.VanillaDB;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.ConstantRange;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.buffer.BufferMgr;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.concurrency.LockAbortException;


/**
 * A B-tree implementation of {@link Index}.
 */
public class BTreeIndex extends Index {
	static final int OP_SEARCH = 0, OP_INSERT = 1, OP_DELETE = 2;

	private Transaction tx;
	private TableInfo dirTi, leafTi;
	private BTreeLeaf leaf = null;
	private BlockId rootBlk;
	private String dataFileName;
	private BufferMgr bufferMgr = VanillaDB.bufferMgr();
	private Type dataType;

	public static long searchCost(Type fldType, long totRecs, long matchRecs) {
		int dirRpb = BLOCK_SIZE
				/ new TableInfo("", BTreeDir.schema(fldType)).recordSize();
		int leafRpb = BLOCK_SIZE
				/ new TableInfo("", BTreeLeaf.schema(fldType)).recordSize();
		long leafs = (int) Math.ceil((double) totRecs / leafRpb);
		long matchLeafs = (int) Math.ceil((double) matchRecs / leafRpb);
		return (long) Math.ceil(Math.log(leafs) / Math.log(dirRpb))
				+ matchLeafs;
	}

	/**
	 * Opens a B-tree index for the specified index. The method determines the
	 * appropriate files for the leaf and directory records, creating them if
	 * they did not exist.
	 * 
	 * @param dataFileName
	 *            the name of data file
	 * @param idxName
	 *            the name of the index
	 * @param fldType
	 *            the type of the indexed field
	 * @param tx
	 *            the calling transaction
	 */
	public BTreeIndex(String dataFileName, String idxName, Type fldType,
			Transaction tx) {
		this.dataFileName = dataFileName;
		this.tx = tx;
		this.dataType = fldType;
		// deal with the leaves
		String leafTbl = idxName + "leaf";
		this.leafTi = new TableInfo(leafTbl, BTreeLeaf.schema(fldType));

		try {
			tx.concurrencyMgr().sLock(leafTi.fileName());
		} catch (LockAbortException e) {
			tx.rollback();
			throw e;
		}
		if (VanillaDB.fileMgr().size(leafTi.fileName()) == 0) {
			// initial the first block in leaf
			try {
				tx.concurrencyMgr().xLock(leafTi.fileName());
			} catch (LockAbortException e) {
				tx.rollback();
				throw e;
			}
			Buffer buff = bufferMgr.pinNew(leafTi.fileName(),
					new BTPageFormatter(leafTi, new long[] { -1, -1 }),
					tx.getTransactionNumber());
			bufferMgr.unpin(tx.getTransactionNumber(), buff);
		}

		// deal with the directory
		String dirTbl = idxName + "dir";
		this.dirTi = new TableInfo(dirTbl, BTreeDir.schema(fldType));
		this.rootBlk = new BlockId(dirTi.fileName(), 0);

		try {
			tx.concurrencyMgr().sLock(dirTi.fileName());
		} catch (LockAbortException e) {
			tx.rollback();
			throw e;
		}
		if (VanillaDB.fileMgr().size(dirTi.fileName()) == 0) {
			try {
				tx.concurrencyMgr().xLock(dirTi.fileName());
			} catch (LockAbortException e) {
				tx.rollback();
				throw e;
			}
			// create new root block
			Buffer buff = bufferMgr.pinNew(dirTi.fileName(),
					new BTPageFormatter(dirTi, new long[] { 0 }),
					tx.getTransactionNumber());
			bufferMgr.unpin(tx.getTransactionNumber(), buff);
		}

		BTreePage rootpage = new BTreePage(dataFileName, rootBlk,
				BTreeDir.NUM_FLAGS, dirTi, tx);
		if (rootpage.getNumRecords() == 0) {
			// insert initial directory entry
			Constant minval = dataType.minValue();
			BTreeDir.insert(rootpage, 0, minval, 0);
		}
		rootpage.close();
	}

	/**
	 * Traverses the directory to find the leaf page corresponding to the lower
	 * bound of the specified key range. The method then position the page
	 * before the first record (if any) matching the that lower bound. The leaf
	 * page is kept open, for use by the methods {@link #next} and
	 * {@link #getDataRecordId}.
	 * 
	 * @see Index#beforeFirst
	 */
	@Override
	public void beforeFirst(ConstantRange searchRange) {
		if (!searchRange.isValid())
			return;

		traverseIndexWithCrabbing(searchRange, OP_SEARCH);
	}

	/**
	 * Moves to the next index record in B-tree leaves matching the
	 * previously-specified search key. Returns false if there are no more such
	 * records.
	 * 
	 * @see Index#next
	 */
	@Override
	public boolean next() {
		return leaf == null ? false : leaf.next();
	}

	/**
	 * Returns the data record ID from the current index record in B-tree
	 * leaves.
	 * 
	 * @see Index#getDataRecordId()
	 */
	@Override
	public RecordId getDataRecordId() {
		return leaf.getDataRecordId();
	}

	/**
	 * Inserts the specified record into the index. The method first traverses
	 * the directory to find the appropriate leaf page; then it inserts the
	 * record into the leaf. If the insertion causes the leaf to split, then the
	 * method calls insert on the root, passing it the directory entry of the
	 * new leaf page. If the root node splits, then {@link BTreeDir#makeNewRoot}
	 * is called.
	 * 
	 * @see Index#insert(Constant, RecordId)
	 */
	@Override
	public void insert(Constant key, RecordId dataRecordId) {
		traverseIndexWithCrabbing(ConstantRange.newInstance(key), OP_INSERT);
		DirEntry e = leaf.insert(dataRecordId);
		close();
		if (e == null)
			return;
		BTreeDir root = new BTreeDir(dataFileName, rootBlk, dirTi, tx);
		DirEntry e2 = root.insert(e);
		if (e2 != null)
			root.makeNewRoot(e2);
		root.close();
	}

	/**
	 * Deletes the specified index record. The method first traverses the
	 * directory to find the leaf page containing that record; then it deletes
	 * the record from the page. F
	 * 
	 * @see Index#delete(Constant, RecordId)
	 */
	@Override
	public void delete(Constant key, RecordId dataRecordId) {
		traverseIndexWithCrabbing(ConstantRange.newInstance(key), OP_DELETE);
		leaf.delete(dataRecordId);
		close();
	}

	/**
	 * Closes the index by closing its open leaf page, if necessary.
	 * 
	 * @see Index#close()
	 */
	@Override
	public void close() {
		if (leaf != null) {
			leaf.close();
			leaf = null;
		}
	}

	private void traverseIndexWithCrabbing(ConstantRange searchRange, int action) {
		BlockId leafblk;
		/*
		 * Search from the first leaf block if there's no lower bound in the
		 * search range.
		 */
		if (!searchRange.hasLowerBound())
			leafblk = new BlockId(dataFileName, 0);
		else {
			BTreeDir root = new BTreeDir(dataFileName, rootBlk, dirTi, tx);
			leafblk = root.search(searchRange.low(), action, leafTi);
			root.close();
		}
		leaf = new BTreeLeaf(dataFileName, leafblk, leafTi, searchRange, tx);
	}
}
