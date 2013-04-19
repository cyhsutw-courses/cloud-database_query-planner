package org.vanilladb.core.query.planner.opt;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.VARCHAR;

import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.parse.CreateViewData;
import org.vanilladb.core.query.parse.Parser;
import org.vanilladb.core.query.parse.QueryData;
import org.vanilladb.core.query.planner.BasicUpdatePlanner;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDB;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.metadata.MetadataMgr;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.tx.Transaction;

public class AdvanceQueryPlannerTest {

	private static Logger logger = Logger
			.getLogger(AdvanceQueryPlannerTest.class.getName());

	@BeforeClass
	public static void init() {
		ServerInit.initData();

		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN ADV QUERY PLANNER TEST");
	}

	@Before
	public void setup() {

	}

	@Test
	public void testQuery() {
		Transaction tx = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, true);

		String qry = "select sid, sname, majorid from student, dept "
				+ "where majorid=did and dname='dept0'";
		Parser psr = new Parser(qry);
		QueryData qd = psr.query();
		Plan p = new HeuristicQueryPlanner().createPlan(qd, tx);
		Schema sch = p.schema();
		assertTrue(
				"*****AdvQueryPlannerTest: bad heuristic plan schema",
				sch.fields().size() == 3 && sch.hasField("sid")
						&& sch.hasField("sname") && sch.hasField("majorid"));
		Scan s = p.open();
		s.beforeFirst();
		while (s.next())
			assertEquals(
					"*****AdvQueryPlannerTest: bad heuristic plan selection",
					(Integer) 0, (Integer) s.getVal("majorid").asJavaVal());
		s.close();
		tx.commit();
	}

	@Test
	public void testView() {
		Transaction tx = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);

		String viewDef = "select sid, sname, majorid from student, dept "
				+ "where majorid=did and dname='dept0'";
		String cmd = "create view view02 as " + viewDef;
		Parser psr = new Parser(cmd);
		CreateViewData cvd = (CreateViewData) psr.updateCommand();
		int i = new BasicUpdatePlanner().executeCreateView(cvd, tx);
		assertTrue("*****PlannerTest: bad create view", i == 0);

		String qry = "select sid, sname, majorid from view02";
		psr = new Parser(qry);
		QueryData qd = psr.query();
		Plan p = new HeuristicQueryPlanner().createPlan(qd, tx);
		Schema sch = p.schema();
		assertTrue(
				"*****PlannerTest: bad view schema",
				sch.fields().size() == 3 && sch.hasField("sid")
						&& sch.hasField("sname") && sch.hasField("majorid"));
		Scan s = p.open();
		s.beforeFirst();
		while (s.next())
			assertEquals(
					"*****PlannerTest: bad basic plan selection from view",
					(Integer) 0, (Integer) s.getVal("majorid").asJavaVal());
		s.close();
		tx.commit();
	}

	@Test
	public void testJoinQuery() {
		Transaction tx = VanillaDB.transaction(
				Connection.TRANSACTION_SERIALIZABLE, false);
		// initial data
		MetadataMgr md = VanillaDB.mdMgr();
		Schema sch = new Schema();
		sch.addField("aid", INTEGER);
		// ach.addStringField("aname", 20);
		sch.addField("acid", BIGINT);
		md.createTable("atable", sch, tx);
		TableInfo ti = md.getTableInfo("atable", tx);

		RecordFile rf = ti.open(tx);
		rf.beforeFirst();
		while (rf.next())
			rf.delete();
		rf.beforeFirst();
		for (int id = 1; id < 9; id++) {
			rf.insert();
			rf.setVal("aid", new IntegerConstant(id));
			// rf.setString("title", "course" + id);
			if (id < 5)
				rf.setVal("acid", new BigIntConstant(10));
			else
				rf.setVal("acid", new BigIntConstant(20));
		}
		rf.close();

		sch = new Schema();
		sch.addField("cid", INTEGER);
		sch.addField("cname", VARCHAR(20));
		sch.addField("ctid", BIGINT);
		md.createTable("ctable", sch, tx);
		ti = md.getTableInfo("ctable", tx);

		rf = ti.open(tx);
		rf.beforeFirst();
		while (rf.next())
			rf.delete();
		rf.beforeFirst();
		rf.insert();
		rf.setVal("cid", new IntegerConstant(10));
		rf.setVal("cname", new VarcharConstant("course10"));
		rf.setVal("ctid", new BigIntConstant(7));
		rf.insert();
		rf.setVal("cid", new IntegerConstant(20));
		rf.setVal("cname", new VarcharConstant("course20"));
		rf.setVal("ctid", new BigIntConstant(9));
		rf.insert();
		rf.setVal("cid", new IntegerConstant(30));
		rf.setVal("cname", new VarcharConstant("course30"));
		rf.setVal("ctid", new BigIntConstant(7));
		rf.close();

		sch = new Schema();
		sch.addField("tid", BIGINT);
		sch.addField("tname", VARCHAR(20));
		md.createTable("ttable", sch, tx);
		ti = md.getTableInfo("ttable", tx);

		rf = ti.open(tx);
		rf.beforeFirst();
		while (rf.next())
			rf.delete();
		rf.beforeFirst();
		rf.insert();
		rf.setVal("tid", new BigIntConstant(7));
		rf.setVal("tname", new VarcharConstant("teacher7"));
		rf.insert();
		rf.setVal("tid", new BigIntConstant(9));
		rf.setVal("tname", new VarcharConstant("teacher9"));
		rf.insert();
		rf.setVal("tid", new BigIntConstant(30));
		rf.setVal("tname", new VarcharConstant("teacher30"));
		rf.close();

		String qry = "select tname, aid,cname from atable, ctable, ttable "
				+ "where acid=cid and ctid=tid and tid>7 ";
		// order by sid desc group by sid
		Parser psr = new Parser(qry);
		QueryData qd = psr.query();

		Plan p = new HeuristicQueryPlanner().createPlan(qd, tx);
		sch = p.schema();

		assertTrue(
				"*****AdvQueryPlannerTest: bad heuristic plan schema",
				sch.fields().size() == 3 && sch.hasField("aid")
						&& sch.hasField("tname") && sch.hasField("cname"));

		Scan s = p.open();
		s.beforeFirst();
		while (s.next()) {

			System.out.println("aid:" + s.getVal("aid") + " cname:"
					+ s.getVal("cname") + " tname:" + s.getVal("tname"));
		}

		s.close();
		tx.commit();
	}

}