package org.vanilladb.core.storage.record;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.VARCHAR;
import static org.vanilladb.core.storage.file.Page.BLOCK_SIZE;
import static org.vanilladb.core.storage.record.RecordPage.EMPTY;

import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDB;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.buffer.BufferMgr;
import org.vanilladb.core.storage.buffer.PageFormatter;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.file.Page;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.tx.Transaction;

public class RecordTest {
	private static Logger logger = Logger.getLogger(RecordTest.class.getName());

	private static String tableName = "testcourse";
	private static Schema schema;
	private static TableInfo ti;
	private BufferMgr bufferMgr = VanillaDB.bufferMgr();

	@BeforeClass
	public static void init() {
		ServerInit.initData();

		schema = new Schema();
		schema.addField("cid", INTEGER);
		schema.addField("title", VARCHAR(20));
		schema.addField("deptid", INTEGER);
		ti = new TableInfo(tableName, schema);

		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN RECORD TEST");
	}

	@Before
	public void setup() {

	}

	@Test
	public void testReadOnly() {
		Transaction tx = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, true);
		String fileName = ti.fileName();
		TestFormatter fmtr = new TestFormatter(ti);
		Buffer buff = bufferMgr.pinNew(fileName, fmtr,
				tx.getTransactionNumber());
		bufferMgr.unpin(tx.getTransactionNumber(), buff);
		RecordPage rp = new RecordPage(buff.block(), ti, tx, true);
		try {
			rp.insert();
			fail("*****RecordTest: bad readOnly");
		} catch (UnsupportedOperationException e) {

		}
	}

	@Test
	public void testRecordPage() {
		Transaction tx = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		String fileName = ti.fileName();
		TestFormatter fmtr = new TestFormatter(ti);
		Buffer buff = bufferMgr.pinNew(fileName, fmtr,
				tx.getTransactionNumber());
		bufferMgr.unpin(tx.getTransactionNumber(), buff);
		RecordPage rp = new RecordPage(buff.block(), ti, tx, true);
		int startid = 0;
		BlockId blk = buff.block();
		// Part 0: Delete existing records (if any)
		while (rp.next())
			rp.delete();
		rp = new RecordPage(blk, ti, tx, true);

		// Part 1: Fill the page with some records
		int id = startid;
		int numinserted = 0;
		while (rp.insert()) {
			rp.setVal("cid", new IntegerConstant(id));
			rp.setVal("deptid", new IntegerConstant((id % 3 + 1) * 10));
			rp.setVal("title", new VarcharConstant("course" + id));
			id++;
			numinserted++;
		}
		rp.close();

		// Part 2: Retrieve the records
		rp = new RecordPage(blk, ti, tx, true);
		id = startid;
		while (rp.next()) {
			int deptid = (Integer) rp.getVal("deptid").asJavaVal();
			int cid = (Integer) rp.getVal("cid").asJavaVal();
			String title = (String) rp.getVal("title").asJavaVal();
			assertTrue("*****RecordTest: bad page read",
					cid == id && title.equals("course" + id)
							&& deptid == (id % 3 + 1) * 10);
			id++;
		}
		rp.close();

		// Part 3: Modify the records
		rp = new RecordPage(blk, ti, tx, true);
		id = startid;
		int numdeleted = 0;
		while (rp.next()) {
			if (rp.getVal("deptid").equals(new IntegerConstant(30))) {
				rp.delete();
				numdeleted++;
			}
		}
		rp.close();
		assertEquals("*****RecordTest: deleted wrong records from page",
				numinserted / 3, numdeleted);

		rp = new RecordPage(blk, ti, tx, true);
		while (rp.next()) {
			assertNotSame("*****RecordTest: bad page delete", (Integer) 30,
					(Integer) rp.getVal("deptid").asJavaVal());
		}
		rp.close();
		tx.rollback();
	}

	@Test
	public void testRecordFile() {
		Transaction tx = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		RecordFile rf = ti.open(tx);
		rf.beforeFirst();
		int max = 300;

		// Part 0: Delete existing records (if any)
		while (rf.next())
			rf.delete();
		rf.beforeFirst();

		// Part 1: Fill the file with lots of records
		for (int id = 0; id < max; id++) {
			rf.insert();
			rf.setVal("cid", new IntegerConstant(id));
			rf.setVal("title", new VarcharConstant("course" + id));
			rf.setVal("deptid", new IntegerConstant((id % 3 + 1) * 10));
		}
		rf.close();

		// Part 2: Retrieve the records
		int id = 0;
		rf = ti.open(tx);
		rf.beforeFirst();
		while (rf.next()) {
			int cid = (Integer) rf.getVal("cid").asJavaVal();
			String title = (String) rf.getVal("title").asJavaVal();
			int deptid = (Integer) rf.getVal("deptid").asJavaVal();
			assertTrue("*****RecordTest: bad file read",
					cid == id && title.equals("course" + id)
							&& deptid == (id % 3 + 1) * 10);
			id++;
		}
		rf.close();
		assertEquals("*****RecordTest: wrong number of records", max, id);

		// Part 3: Delete some of the records
		rf = ti.open(tx);
		rf.beforeFirst();
		int numdeleted = 0;
		while (rf.next()) {
			if (rf.getVal("deptid").equals(new IntegerConstant(30))) {
				rf.delete();
				numdeleted++;
			}
		}
		assertEquals("*****RecordTest: wrong number of deletions", max / 3,
				numdeleted);

		// test that the deletions occurred
		rf.beforeFirst();
		while (rf.next()) {
			assertNotSame("*****RecordTest: not enough deletions",
					(Integer) 30, (Integer) rf.getVal("deptid").asJavaVal());
		}
		rf.close();
		tx.rollback();
	}

}

class TestFormatter implements PageFormatter {
	private static int intTypeCapacity = Page.maxSize(INTEGER);
	private TableInfo ti;

	public TestFormatter(TableInfo ti) {
		this.ti = ti;
	}

	@Override
	public void format(Page page) {
		int recsize = ti.recordSize() + intTypeCapacity;
		for (int pos = 0; pos + recsize <= BLOCK_SIZE; pos += recsize) {
			page.setVal(pos, new IntegerConstant(EMPTY));
			makeDefaultRecord(page, pos);
		}
	}

	private void makeDefaultRecord(Page page, int pos) {
		for (String fldname : ti.schema().fields()) {
			int offset = ti.offset(fldname);
			if (ti.schema().type(fldname).equals(INTEGER))
				page.setVal(pos + intTypeCapacity + offset,
						new IntegerConstant(0));
			else
				page.setVal(pos + intTypeCapacity + offset,
						new VarcharConstant(""));
		}
	}
}
