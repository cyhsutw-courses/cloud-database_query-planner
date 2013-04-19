package org.vanilladb.core.storage.index.hash;

import static org.junit.Assert.assertTrue;
import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.VARCHAR;
import static org.vanilladb.core.storage.index.Index.IDX_HASH;

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

public class HashIndexTest {
	private static Logger logger = Logger.getLogger(HashIndexTest.class
			.getName());
	private static MetadataMgr md;
	private static String dataTableName = "_tempHITable";

	@BeforeClass
	public static void init() {
		ServerInit.initData();
		md = VanillaDB.mdMgr();

		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN HASHINDEX TEST");

		Transaction tx = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		Schema schema = new Schema();
		schema.addField("cid", INTEGER);
		schema.addField("title", VARCHAR(20));
		schema.addField("deptid", INTEGER);
		md.createTable(dataTableName, schema, tx);
		md.createIndex("_tempHI1", dataTableName, "cid", IDX_HASH, tx);
		md.createIndex("_tempHI2", dataTableName, "title", IDX_HASH, tx);
		md.createIndex("_tempHI3", dataTableName, "deptid", IDX_HASH, tx);

		tx.commit();
	}

	@Before
	public void setup() {

	}

	@Test
	public void testHashIndex() {
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

		cidIndex.beforeFirst(ConstantRange.newInstance(new IntegerConstant(5)));
		int k = 0;
		while (cidIndex.next())
			k++;
		assertTrue("*****HashIndexTest: bad insert", k == 10);

		cidIndex.beforeFirst(ConstantRange.newInstance(new IntegerConstant(7)));
		cidIndex.next();
		assertTrue("*****HashIndexTest: bad read index", cidIndex
				.getDataRecordId().equals(rid2));

		for (int i = 0; i < 10; i++)
			cidIndex.delete(int5, records[i]);
		cidIndex.beforeFirst(ConstantRange.newInstance(new IntegerConstant(5)));
		assertTrue("*****HashIndexTest: bad delete", cidIndex.next() == false);

		cidIndex.delete(int7, rid2);
		cidIndex.close();
		tx.commit();
	}
}
