package org.vanilladb.core.storage.index.btree;

import static org.junit.Assert.assertTrue;
import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.VARCHAR;
import static org.vanilladb.core.storage.index.Index.IDX_BTREE;

import java.sql.Connection;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDB;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.ConstantRange;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.metadata.MetadataMgr;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;

public class BTreeIndexTest {
	private static Logger logger = Logger.getLogger(BTreeIndexTest.class
			.getName());
	private static MetadataMgr md;
	private static String dataTableName = "_tempBtreeData";

	@BeforeClass
	public static void init() {
		ServerInit.initData();
		md = VanillaDB.mdMgr();

		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN BTREEINDEX TEST");

		Transaction tx = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		Schema schema = new Schema();
		schema.addField("cid", INTEGER);
		schema.addField("title", VARCHAR(20));
		schema.addField("deptid", INTEGER);
		md.createTable(dataTableName, schema, tx);
		md.createIndex("_tempI1", dataTableName, "cid", IDX_BTREE, tx);
		md.createIndex("_tempI2", dataTableName, "title", IDX_BTREE, tx);
		md.createIndex("_tempI3", dataTableName, "deptid", IDX_BTREE, tx);
		tx.commit();
	}

	@Before
	public void setup() {

	}

	@Test
	public void testBasicOperation() {
		Transaction tx = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		Map<String, IndexInfo> idxmap = md.getIndexInfo(dataTableName, tx);
		Index cidIndex = idxmap.get("cid").open(tx);
		RecordId[] records = new RecordId[10];
		BlockId blk = new BlockId(dataTableName + ".tbl", 0);
		Constant int5 = new IntegerConstant(5);
		for (int i = 0; i < 10; i++) {
			records[i] = new RecordId(blk, i);
			cidIndex.insert(int5, records[i]);
		}

		RecordId rid2 = new RecordId(blk, 9);
		Constant int7 = new IntegerConstant(7);
		cidIndex.insert(int7, rid2);

		cidIndex.beforeFirst(ConstantRange.newInstance(int5));
		int k = 0;
		while (cidIndex.next())
			k++;
		assertTrue("*****BTreeIndexTest: bad insert", k == 10);

		cidIndex.beforeFirst(ConstantRange.newInstance(int7));
		cidIndex.next();
		assertTrue("*****BTreeIndexTest: bad read index", cidIndex
				.getDataRecordId().equals(rid2));

		for (int i = 0; i < 10; i++)
			cidIndex.delete(int5, records[i]);
		cidIndex.beforeFirst(ConstantRange.newInstance(int5));
		assertTrue("*****BTreeIndexTest: bad delete", cidIndex.next() == false);

		cidIndex.delete(int7, rid2);
		cidIndex.close();
		tx.commit();
	}

	@Test
	public void testBTreeIndex() {
		Transaction tx = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		Map<String, IndexInfo> idxmap = md.getIndexInfo(dataTableName, tx);
		Index cidIndex = idxmap.get("deptid").open(tx);
		BlockId blk = new BlockId(dataTableName + ".tbl", 0);

		for (int k = 0; k < 60; k++)
			for (int i = 0; i < 40; i++)
				cidIndex.insert(new IntegerConstant(k), new RecordId(blk, k
						* 40 + i));
		int count = 0;
		Constant int7 = new IntegerConstant(7);
		while (count < 500) {
			cidIndex.insert(int7, new RecordId(blk, 2500 + count));
			count++;
		}

		// test larger than
		cidIndex.beforeFirst(ConstantRange.newInstance(new IntegerConstant(50),
				false, null, false));
		int j = 0;
		while (cidIndex.next())
			j++;

		assertTrue("*****BTreeIndexTest: bad > selection", j == 360);

		Constant int5 = new IntegerConstant(5);
		// test less than
		cidIndex.beforeFirst(ConstantRange
				.newInstance(null, false, int5, false));
		j = 0;
		while (cidIndex.next())
			j++;
		assertTrue("*****BTreeIndexTest: bad < selection", j == 200);

		// test equality
		cidIndex.beforeFirst(ConstantRange.newInstance(int5));
		j = 0;
		while (cidIndex.next())
			j++;
		assertTrue("*****BTreeIndexTest: bad equal with", j == 40);

		// test delete
		for (int k = 0; k < 60; k++)
			for (int i = 0; i < 40; i++) {
				cidIndex.delete(new IntegerConstant(k), new RecordId(blk, k
						* 40 + i));
			}
		cidIndex.beforeFirst(ConstantRange.newInstance(int5));
		assertTrue("*****BTreeIndexTest: bad delete", cidIndex.next() == false);

		count = 0;
		while (count < 500) {
			cidIndex.delete(int7, new RecordId(blk, 2500 + count));
			count++;
		}
		cidIndex.beforeFirst(ConstantRange.newInstance(int7));
		assertTrue("*****BTreeIndexTest: bad delete", cidIndex.next() == false);

		cidIndex.close();
		tx.commit();
	}
}
