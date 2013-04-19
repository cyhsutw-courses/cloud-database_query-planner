package org.vanilladb.core.storage.index.hash;

import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.storage.file.Page.BLOCK_SIZE;

import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.ConstantRange;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;


/**
 * A static hash implementation of {@link Index}. A fixed number of buckets is
 * allocated, and each bucket is implemented as a file of index records.
 */
public class HashIndex extends Index {
	/**
	 * A field name of the schema of index records.
	 */
	private static final String SCHEMA_KEY = "key", SCHEMA_RID_BLOCK = "block",
			SCHEMA_RID_ID = "id";

	public static final int NUM_BUCKETS;

	static {
		String prop = System.getProperty(HashIndex.class.getName()
				+ ".NUM_BUCKETS");
		NUM_BUCKETS = (prop == null ? 100 : Integer.parseInt(prop.trim()));
	}

	public static long searchCost(Type fldType, long totRecs, long matchRecs) {
		TableInfo idxti = new TableInfo("", schema(fldType));
		int rpb = BLOCK_SIZE / idxti.recordSize();
		return (totRecs / rpb) / NUM_BUCKETS;
	}

	/**
	 * Returns the schema of the index records.
	 * 
	 * @param fldType
	 *            the type of the indexed field
	 * 
	 * @return the schema of the index records
	 */
	private static Schema schema(Type fldType) {
		Schema sch = new Schema();
		sch.addField(SCHEMA_KEY, fldType);
		sch.addField(SCHEMA_RID_BLOCK, BIGINT);
		sch.addField(SCHEMA_RID_ID, INTEGER);
		return sch;
	}

	private Type fldType;
	private String idxName, dataFileName;
	private Transaction tx;
	private Constant searchKey = null;
	private RecordFile rf = null;

	/**
	 * Opens a hash index for the specified index.
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
	public HashIndex(String dataFileName, String idxName, Type fldType,
			Transaction tx) {
		this.dataFileName = dataFileName;
		this.idxName = idxName;
		this.fldType = fldType;
		this.tx = tx;
	}

	/**
	 * Positions the index before the first index record having the specified
	 * search key. The method hashes the search key to determine the bucket, and
	 * then opens a table scan on the file corresponding to the bucket. The
	 * table scan for the previous bucket (if any) is closed.
	 * 
	 * @see Index#beforeFirst(Constant)
	 */
	@Override
	public void beforeFirst(ConstantRange searchRange) {
		close();
		if (!searchRange.isConstant())
			throw new UnsupportedOperationException();

		this.searchKey = searchRange.asConstant();
		int bucket = searchKey.hashCode() % NUM_BUCKETS;
		String tblname = idxName + bucket;
		TableInfo ti = new TableInfo(tblname, schema(fldType));
		this.rf = ti.open(tx);
		rf.beforeFirst();
	}

	/**
	 * Moves to the next index record having the search key.
	 * 
	 * @see Index#next()
	 */
	@Override
	public boolean next() {
		while (rf.next())
			if (rf.getVal(SCHEMA_KEY).compareTo(searchKey) == 0)
				return true;
		return false;
	}

	/**
	 * Retrieves the data record ID from the current index record.
	 * 
	 * @see Index#getDataRecordId()
	 */
	@Override
	public RecordId getDataRecordId() {
		long blkNum = (Long) rf.getVal(SCHEMA_RID_BLOCK).asJavaVal();
		int id = (Integer) rf.getVal(SCHEMA_RID_ID).asJavaVal();
		return new RecordId(new BlockId(dataFileName, blkNum), id);
	}

	/**
	 * Inserts a new index record into this index.
	 * 
	 * @see Index#insert(Constant, RecordId)
	 */
	@Override
	public void insert(Constant key, RecordId dataRecordId) {
		beforeFirst(ConstantRange.newInstance(key));
		rf.insert();
		rf.setVal(SCHEMA_KEY, key);
		rf.setVal(SCHEMA_RID_BLOCK, new BigIntConstant(dataRecordId.block()
				.number()));
		rf.setVal(SCHEMA_RID_ID, new IntegerConstant(dataRecordId.id()));
	}

	/**
	 * Deletes the specified index record.
	 * 
	 * @see Index#delete(Constant, RecordId)
	 */
	@Override
	public void delete(Constant key, RecordId dataRecordId) {
		beforeFirst(ConstantRange.newInstance(key));
		while (next())
			if (getDataRecordId().equals(dataRecordId)) {
				rf.delete();
				return;
			}
	}

	/**
	 * Closes the index by closing the current table scan.
	 * 
	 * @see Index#close()
	 */
	@Override
	public void close() {
		if (rf != null)
			rf.close();
	}
}
