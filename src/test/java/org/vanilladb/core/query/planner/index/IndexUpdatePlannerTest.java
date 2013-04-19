package org.vanilladb.core.query.planner.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.VARCHAR;
import static org.vanilladb.core.storage.index.Index.IDX_BTREE;

import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.parse.DeleteData;
import org.vanilladb.core.query.parse.InsertData;
import org.vanilladb.core.query.parse.ModifyData;
import org.vanilladb.core.query.parse.Parser;
import org.vanilladb.core.query.parse.QueryData;
import org.vanilladb.core.query.planner.opt.HeuristicQueryPlanner;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDB;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.MetadataMgr;
import org.vanilladb.core.storage.tx.Transaction;

public class IndexUpdatePlannerTest {
	private static Logger logger = Logger
			.getLogger(IndexUpdatePlannerTest.class.getName());
	private static String tableName = "indextest";

	@BeforeClass
	public static void init() {
		ServerInit.initData();

		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN INDEX UPDATE PLANNER TEST");

		// create and populate the indexed temp table
		MetadataMgr md = VanillaDB.mdMgr();
		Transaction tx = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		Schema sch = new Schema();
		sch.addField("tid", INTEGER);
		sch.addField("tname", VARCHAR(10));
		sch.addField("tdate", BIGINT);
		md.createTable(tableName, sch, tx);
		md.createIndex("_tempIUP1", tableName, "tid", IDX_BTREE, tx);
		md.createIndex("_tempIUP2", tableName, "tdate", IDX_BTREE, tx);

		tx.commit();

	}

	@Before
	public void setup() {

	}

	@Test
	public void testInsert() {
		Transaction tx = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);

		String cmd = "insert into indextest(tid,tname,tdate) values(1, 'basketry',9890033330000)";
		Parser psr = new Parser(cmd);
		InsertData id = (InsertData) psr.updateCommand();
		int n = new IndexUpdatePlanner().executeInsert(id, tx);
		if (n != 1)
			assertEquals(
					"*****IndexUpdatePlannerTest: bad insertion return value",
					1, n);

		String qry = "select tid, tname, tdate from indextest where tid = 1";
		psr = new Parser(qry);
		QueryData qd = psr.query();
		Plan p = new HeuristicQueryPlanner().createPlan(qd, tx);
		Scan s = p.open();
		s.beforeFirst();
		int insertcount = 0;

		while (s.next()) {
			assertEquals("*****IndexUpdatePlannerTest: bad insert retrieval",
					(Integer) 1, (Integer) s.getVal("tid").asJavaVal());
			assertEquals("*****IndexUpdatePlannerTest: bad insert retrieval",
					"basketry", (String) s.getVal("tname").asJavaVal());
			assertEquals("*****IndexUpdatePlannerTest: bad insert retrieval",
					(Long) 9890033330000L, (Long) s.getVal("tdate").asJavaVal());
			insertcount++;
		}
		s.close();
		assertEquals("*****IndexUpdatePlannerTest: bad insertion count", 1,
				insertcount);

		tx.rollback();
	}

	@Test
	public void testDelete() {
		Transaction tx = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		String cmd = "delete from indextest where tid = 1";
		Parser psr = new Parser(cmd);
		DeleteData dd = (DeleteData) psr.updateCommand();
		IndexUpdatePlanner iup = new IndexUpdatePlanner();
		iup.executeDelete(dd, tx);

		String qry = "select tid from indextest where tid = 1";
		psr = new Parser(qry);
		QueryData qd = psr.query();
		Plan p = new HeuristicQueryPlanner().createPlan(qd, tx);
		Scan s = p.open();
		s.beforeFirst();
		while (s.next())
			fail("*****IndexUpdatePlannerTest: bad delete");
		s.close();
		tx.rollback();
	}

	@Test
	public void testModify() {
		Transaction tx = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		String cmd = "insert into indextest(tid,tname,tdate) values(2, 'ggg',789000)";
		Parser psr = new Parser(cmd);
		InsertData id = (InsertData) psr.updateCommand();
		int n = new IndexUpdatePlanner().executeInsert(id, tx);

		cmd = "update indextest set tname = 'kkk', tdate=999999999 where tid = 2";
		psr = new Parser(cmd);
		ModifyData md = (ModifyData) psr.updateCommand();
		n = new IndexUpdatePlanner().executeModify(md, tx);
		assertTrue("*****IndexUpdatePlannerTest: bad insertion", n > 0);

		String qry = "select tid, tname,tdate from indextest tid = 2";
		psr = new Parser(qry);
		QueryData qd = psr.query();
		Plan p = new HeuristicQueryPlanner().createPlan(qd, tx);
		Scan s = p.open();
		s.beforeFirst();
		boolean modi = true;
		while (s.next()) {
			if ((Integer) s.getVal("tid").asJavaVal() == 2
					&& !((String) s.getVal("tname").asJavaVal()).equals("kkk")
					&& (Long) s.getVal("tdate").asJavaVal() == 999999999L)
				modi = false;
		}
		s.close();
		assertEquals("*****IndexUpdatePlannerTest: wrong records modified",
				true, modi);

		tx.rollback();
	}
}
