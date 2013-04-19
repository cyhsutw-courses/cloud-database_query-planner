package org.vanilladb.core.storage.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
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
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.tx.Transaction;

public class MetadataTest {
	private static Logger logger = Logger.getLogger(MetadataTest.class
			.getName());

	private static MetadataMgr md;

	@BeforeClass
	public static void init() {
		ServerInit.initData();

		md = VanillaDB.mdMgr();

		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN METADATA TEST");
	}

	@Before
	public void setup() {

	}

	@Test
	public void testTableMgr() {
		Transaction tx = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		Schema sch1 = new Schema();
		sch1.addField("A", INTEGER);
		sch1.addField("B", VARCHAR(30));
		Schema sch2 = new Schema();
		sch2.addField("A", INTEGER);
		sch2.addField("C", VARCHAR(10));

		md.createTable("T1", sch1, tx);
		md.createTable("T2", sch2, tx);

		TableInfo ti1 = md.getTableInfo("T1", tx);
		TableInfo ti2 = md.getTableInfo("T2", tx);
		TableInfo ti3 = md.getTableInfo("T3", tx);
		assertTrue("*****MetadataTest: bad table info", ti1.schema().fields()
				.size() == 2
				&& ti1.schema().hasField("A")
				&& ti1.schema().hasField("B")
				&& !ti1.schema().hasField("C"));
		assertTrue("*****MetadataTest: bad table info", ti2.schema().fields()
				.size() == 2
				&& ti2.schema().hasField("A")
				&& ti2.schema().hasField("C")
				&& !ti2.schema().hasField("B"));
		assertTrue("*****MetadataTest: bad table info", ti3 == null);

		tx.rollback();
	}

	@Test
	public void testViewMgr() {
		Transaction tx = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		md.createView("V1", "abcde", tx);
		md.createView("V2", "select * from T", tx);

		String s1 = md.getViewDef("V1", tx);
		String s2 = md.getViewDef("V2", tx);
		String s3 = md.getViewDef("V3", tx);
		assertEquals("*****MetadataTest: bad view info", "abcde", s1);
		assertEquals("*****MetadataTest: bad view info", "select * from T", s2);
		assertNull("*****MetadataTest: bad view info", s3);

		tx.rollback();
	}

	@Test
	public void testIndexMgr() {
		Transaction tx = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		Schema sch = new Schema();
		sch.addField("A", INTEGER);
		sch.addField("B", VARCHAR(20));
		sch.addField("C", INTEGER);
		md.createTable("IdxTest", sch, tx);
		md.createIndex("I1", "IdxTest", "A", IDX_HASH, tx);
		md.createIndex("I2", "IdxTest", "B", IDX_HASH, tx);
		md.createIndex("I3", "IdxTest", "C", IDX_HASH, tx);

		Map<String, IndexInfo> idxmap = md.getIndexInfo("IdxTest", tx);
		assertTrue("*****MetadataTest: bad index info", idxmap.containsKey("A")
				&& idxmap.containsKey("B") && idxmap.containsKey("C")
				&& idxmap.keySet().size() == 3);

		// check for index open success and properties setting
		Index k = idxmap.get("A").open(tx);
		assertTrue("*****MetadataTest: bad index open", k != null);
		tx.rollback();
	}
}
